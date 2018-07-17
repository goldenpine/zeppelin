/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.remote;

import com.google.common.annotations.VisibleForTesting;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages start / stop of Spark remote interpreter process on a Kubernetes cluster.
 * After Spark Driver started by spark-submit is in Running state, creates a Kubernetes service
 * to connect to RemoteInterpreterServer running inside Spark Driver.
 */
public class SparkK8sRemoteInterpreterManagedProcess extends RemoteInterpreterProcess
        implements ExecuteResultHandler {

  private static final Logger logger = LoggerFactory.getLogger(
      SparkK8sRemoteInterpreterManagedProcess.class);

  private static final String SPARK_APP_SELECTOR = "spark-app-selector";
  private static final String DRIVER_SERVICE_NAME_SUFFIX = "-ri-svc";
  private static final String KUBERNETES_NAMESPACE = "default";
  private static final String INTERPRETER_PROCESS_ID = "interpreter-processId";

  protected final String interpreterRunner;
  protected final String portRange;
  private DefaultExecutor executor;
  protected ProcessLogOutputStream processOutput;
  private ExecuteWatchdog watchdog;
  protected AtomicBoolean running = new AtomicBoolean(false);
  protected String host = "localhost";
  protected int port = -1;
  protected final String interpreterDir;
  protected final String localRepoDir;

  protected Map<String, String> env;
  private final String zeppelinServerRPCHost;
  private final int zeppelinServerRPCPort;
  private final String processLabelId;
  private final String interpreterSettingName;
  private final String interpreterGroupId;
  private final boolean isUserImpersonated;

  /**
   * Default url for Kubernetes inside of an Kubernetes cluster.
   */
  private static String K8_URL = "https://kubernetes:443";

  private KubernetesClient kubernetesClient;

  private Service driverService;

  private AtomicBoolean serviceRunning =  new AtomicBoolean(false);

  public SparkK8sRemoteInterpreterManagedProcess(String zeppelinServerRPCHost,
                                                 int zeppelinServerRPCPort,
                                                 String intpRunner,
                                                 String portRange,
                                                 String intpDir,
                                                 String localRepoDir,
                                                 Map<String, String> env,
                                                 int connectTimeout,
                                                 String processLabelId,
                                                 String interpreterSettingName,
                                                 String interpreterGroupId,
                                                 boolean isUserImpersonated) {

    super(connectTimeout);
    this.zeppelinServerRPCHost = zeppelinServerRPCHost;
    this.zeppelinServerRPCPort = zeppelinServerRPCPort;
    this.interpreterRunner = intpRunner;
    this.portRange = portRange;
    this.env = env;
    this.interpreterDir = intpDir;
    this.localRepoDir = localRepoDir;
    this.processLabelId = processLabelId;
    this.port = 30000;
    this.interpreterSettingName = interpreterSettingName;
    this.interpreterGroupId = interpreterGroupId;
    this.isUserImpersonated = isUserImpersonated;
  }


  @Override
  public String getInterpreterSettingName() {
    return interpreterSettingName;
  }

  @Override
  public void start(String userName) {
    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    cmdLine.addArgument("-d", false);
    cmdLine.addArgument(interpreterDir, false);
    cmdLine.addArgument("-c", false);
    cmdLine.addArgument(zeppelinServerRPCHost, false);
    cmdLine.addArgument("-p", false);
    cmdLine.addArgument(Integer.toString(zeppelinServerRPCPort), false);
    cmdLine.addArgument("-r", false);
    cmdLine.addArgument(Integer.toString(port) + ":" + Integer.toString(port),
      false);
    cmdLine.addArgument("-i", false);
    cmdLine.addArgument(interpreterGroupId, false);

    if (isUserImpersonated && !userName.equals("anonymous")) {
      cmdLine.addArgument("-u", false);
      cmdLine.addArgument(userName, false);
    }
    cmdLine.addArgument("-l", false);
    cmdLine.addArgument(localRepoDir, false);
    cmdLine.addArgument("-g", false);
    cmdLine.addArgument(interpreterSettingName, false);

    ByteArrayOutputStream cmdOut = executeCommand(cmdLine);

    synchronized (serviceRunning) {
      try {
        serviceRunning.wait(getConnectTimeout());
      } catch (InterruptedException e) {
        logger.error("wait for connect interrupted", e);
      }
    }

    // try to connect if service is started
    if (serviceRunning.get()) {
      for (int retryCount = 0; !running.get() && retryCount < 10; retryCount++) {
        if (RemoteInterpreterUtils.checkIfRemoteEndpointAccessible(host, port)) {
          logger.info("Remote endpoint accessible at: {}:{}", host, port);
          running.set(true);
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("wait for connect interrupted", e);
        }
      }
    }

    if (!running.get()) {
      String errorMessage = "Unable to start SparkK8RemoteInterpreterManagedProcess: " +
        "Spark Driver not found." + "\n" + new String(cmdOut.toByteArray());
//      if (podStatus != null) {
//        errorMessage = "Unable to start SparkK8RemoteInterpreterManagedProcess: " +
//          "Not able to connect to endpoint, Spark Driver status: " + podStatus + "\n"
//          + new String(cmdOut.toByteArray());
//      }
      stop();
      throw new RuntimeException(errorMessage);
    }

  }

  private KubernetesClient getKubernetesClient() {
    if (kubernetesClient == null) {
      Config config = new ConfigBuilder().withMasterUrl(K8_URL).build();
      logger.info("Connect to Kubernetes cluster at: {}", K8_URL);
      kubernetesClient = new DefaultKubernetesClient(config);
    }
    return kubernetesClient;
  }

  private Service getEndpointService(String serviceName)
      throws KubernetesClientException {
    logger.debug("Check if RemoteInterpreterServer service {} exists", serviceName);
    return getKubernetesClient().services().inNamespace(KUBERNETES_NAMESPACE).withName(serviceName)
      .get();
  }

  private Service getOrCreateEndpointService(Pod driverPod)
      throws KubernetesClientException {
    String serviceName = driverPod.getMetadata().getAnnotations().get("spark-app-name") +
      DRIVER_SERVICE_NAME_SUFFIX;
    driverService = getEndpointService(serviceName);

    // create endpoint service for RemoteInterpreterServer
    if (driverService == null) {
      Map<String, String> labels = driverPod.getMetadata().getLabels();
      String label = labels.get(SPARK_APP_SELECTOR);
      logger.info("Create RemoteInterpreterServer service for spark-app-selector: {}", label);
      driverService = new ServiceBuilder().withNewMetadata()
              .withName(serviceName).endMetadata()
              .withNewSpec().addNewPort().withProtocol("TCP")
              .withPort(getPort()).withNewTargetPort(getPort()).endPort()
              .addToSelector(SPARK_APP_SELECTOR, label)
              .withType("ClusterIP")
              //.withClusterIP("None")
              .endSpec().build();
      driverService = getKubernetesClient().services().inNamespace(KUBERNETES_NAMESPACE)
        .create(driverService);
    }

    return driverService;
  }

  private void deleteEndpointService()
      throws KubernetesClientException {
    if (driverService != null) {
      boolean result = getKubernetesClient().services().inNamespace(KUBERNETES_NAMESPACE)
        .delete(driverService);
      logger.info("Delete RemoteInterpreterServer service {} : {}",
        driverService.getMetadata().getName(), result);
    }
  }

  private void deleteDriverJob() {
    List<Job> list = getKubernetesClient().extensions().jobs().inNamespace
      (KUBERNETES_NAMESPACE)
      .withLabel(INTERPRETER_PROCESS_ID, processLabelId).list().getItems();
    if (list.size() >= 1) {
      Job driverJob = list.iterator().next();
      logger.debug("Driver job: {} {} parallelism: {} completions: {}", driverJob.getMetadata()
          .getName(),
        driverJob.getSpec().getParallelism(),
        driverJob.getSpec().getCompletions());

      if (driverJob.getSpec().getCompletions() == 0) {
        getKubernetesClient().extensions().jobs().delete(driverJob);
      }
    } else {
      logger.debug("Pod not found!");
    }
  }

  protected void stopEndPoint() {
    if (driverService != null) {
      try {
        deleteEndpointService();
        deleteDriverJob();
        getKubernetesClient().close();
        kubernetesClient = null;
      } catch (KubernetesClientException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  protected ByteArrayOutputStream executeCommand(CommandLine cmdLine) {

    executor = new DefaultExecutor();

    ByteArrayOutputStream cmdOut = new ByteArrayOutputStream();
    processOutput = new ProcessLogOutputStream(logger);
    processOutput.setOutputStream(cmdOut);

    executor.setStreamHandler(new PumpStreamHandler(processOutput));
    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    try {
      Map procEnv = EnvironmentUtils.getProcEnvironment();
      procEnv.putAll(env);

      logger.info("Run interpreter process {}", cmdLine);
      executor.execute(cmdLine, procEnv, this);
    } catch (IOException e) {
      running.set(false);
      throw new RuntimeException(e);
    }

    return cmdOut;
  }

  public void stop() {
    if (isRunning()) {
      logger.info("kill interpreter process");
      try {
        callRemoteFunction(new RemoteFunction<Void>() {
          @Override
          public Void call(RemoteInterpreterService.Client client) throws Exception {
            client.shutdown();
            return null;
          }
        });
      } catch (Exception e) {
        logger.warn("ignore the exception when shutting down");
      }
    }
    stopEndPoint();
    executor = null;
    if (watchdog != null) {
      watchdog.destroyProcess();
      watchdog = null;
    }
    running.set(false);
    logger.info("Remote process terminated");
  }

  public void onProcessComplete(int exitValue) {
    logger.info("Interpreter process exited {}", exitValue);
    running.set(false);
    synchronized (serviceRunning) {
      serviceRunning.notifyAll();
    }
  }

  // called by RemoteInterpreterServer to notify that RemoteInterpreter Process is started
  @Override
  public void processStarted(int port, String host) {
    logger.info("RemoteInterpreterServer listening on {} {}", host, port);
    List<Pod> podList = getKubernetesClient().pods().inNamespace(KUBERNETES_NAMESPACE).withLabel
      (INTERPRETER_PROCESS_ID, processLabelId).list().getItems();
    if (podList.size() == 0) {
      logger.warn("No Driver Pod found with label {}={}", INTERPRETER_PROCESS_ID, processLabelId);
      return;
    }
    this.port = port;
    Service driverService  = getOrCreateEndpointService(podList.get(0));
    if (driverService != null) {
      this.host = driverService.getSpec().getClusterIP();
      logger.info("Driver Service created: {}:{}", host, port);
      synchronized (serviceRunning) {
        serviceRunning.set(true);
        serviceRunning.notifyAll();
      }
    }
  }

  public void onProcessFailed(ExecuteException e) {
    logger.info("Interpreter process failed {}", e);
    running.set(false);
    synchronized (serviceRunning) {
      serviceRunning.notifyAll();
    }
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @VisibleForTesting
  public Map<String, String> getEnv() {
    return env;
  }

  @VisibleForTesting
  public String getLocalRepoDir() {
    return localRepoDir;
  }

  @VisibleForTesting
  public String getInterpreterDir() {
    return interpreterDir;
  }

  @VisibleForTesting
  public String getInterpreterRunner() {
    return interpreterRunner;
  }

  public boolean isRunning() {
    return running.get();
  }

  /**
   * ProcessLogOutputStream
   */
  protected static class ProcessLogOutputStream extends LogOutputStream {

    private Logger logger;
    OutputStream out;

    public ProcessLogOutputStream(Logger logger) {
      this.logger = logger;
    }

    @Override
    protected void processLine(String s, int i) {
      this.logger.debug(s);
    }

    @Override
    public void write(byte [] b) throws IOException {
      super.write(b);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b);
          }
        }
      }
    }

    @Override
    public void write(byte [] b, int offset, int len) throws IOException {
      super.write(b, offset, len);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b, offset, len);
          }
        }
      }
    }

    public void setOutputStream(OutputStream out) {
      synchronized (this) {
        this.out = out;
      }
    }
  }

}
