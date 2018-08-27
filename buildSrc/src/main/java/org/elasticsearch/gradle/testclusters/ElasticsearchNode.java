/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.gradle.Distribution;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.gradle.internal.os.OperatingSystem.current;

public class ElasticsearchNode {

    private static final Logger logger = Logging.getLogger(ElasticsearchNode.class);

    private static final int ES_DESTROY_TIMEOUT = 20;
    private static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = SECONDS;
    private static final int NODE_UP_TIMEOUT = 30;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = SECONDS;

    private final String name;
    private final File sharedArtifactsDir;
    private final File rootDir;
    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final GradleServicesAdapter services;

    private Distribution distribution = Distribution.ZIP;
    private Version version = VersionProperties.getElasticsearch();
    private File javaHome;

    private volatile Process esProcess;

    ElasticsearchNode(String name, GradleServicesAdapter services, File sharedArtifactsDir, File workDirBase) {
        this.name = name;
        this.services = services;
        this.sharedArtifactsDir = sharedArtifactsDir;
        this.rootDir = new File(workDirBase, pathSafe(name));
    }

    private static String pathSafe(String name) {
        return name
            .replaceAll("^[\\:#/\\s]+", "")
            .replaceAll("[\\:#/\\s]+", "-");
    }

    private File getWorkDir() {
        return new File(rootDir, getDistribution().getFileName() + "-" + getVersion());
    }

    private File getConfigFile() {
        return new File(getWorkDir(), "config/elasticsearch.yml");
    }

    private File getConfPathData() {
        return new File(getWorkDir(), "data");
    }

    private File getConfPathSharedData() {
        return new File(getWorkDir(), "sharedData");
    }

    private File getConfPathRepo() {
        return new File(getWorkDir(), "repo");
    }

    private File getLogsDir() {
        return new File(getWorkDir(), "logs");
    }

    private File getHttpPortsFile() {
        return new File(getLogsDir(), "http.ports");
    }

    private File getTransportPortFile() {
        return new File(getLogsDir(), "transport.ports");
    }

    public String getName() {
        return name;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        Objects.requireNonNull(version, "Can't configure a null version");
        checkNotRunning();
        this.version = version;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    public void setDistribution(Distribution distribution) {
        Objects.requireNonNull(distribution, "Can't configure a null distribution");
        checkNotRunning();
        this.distribution = distribution;
    }

    public void setJavaHome(File javaHome) {
        Objects.requireNonNull(javaHome, "null javaHome passed to cluster formation");
        checkNotRunning();
        if (javaHome.exists() == false) {
            throw new TestClustersException("java home does not exists `" + javaHome + "`");
        }
        this.javaHome = javaHome;
    }

    public File getJavaHome() {
        return javaHome;
    }

    public String getHttpSocketURI() {
        waitForClusterHealthYellow();
        return getHttpPortInternal().get(0);
    }

    public String getTransportPortURI() {
        waitForClusterHealthYellow();
        return getTransportPortInternal().get(0);
    }

    public File getCopyOfConfDir() {
        File configDir = getConfigFile().getParentFile();
        File copyOfConfigDir = new File(getWorkDir(), "copy-of-config");
        services.sync(spec -> {
            spec.from(services.fileTree(configDir));
            spec.setFileMode(0444);
            spec.into(copyOfConfigDir);
        });
        return copyOfConfigDir;
    }

    void claim() {
        int currentClaims = noOfClaims.incrementAndGet();
        logger.debug("{} registered new claim", this);
    }

    /**
     * Start the cluster if not running. Does nothing if the cluster is already running.
     */
    void start() {
        if (started.getAndSet(true)) {
            logger.info("Already started `{}`", this);
            return;
        } else {
            logger.lifecycle("Starting `{}`", this);
        }

        File artifact = new File(
            sharedArtifactsDir,
            distribution.getFileName() + "-" + getVersion() + "." + distribution.getExtension()
        );
        if (artifact.exists() == false) {
            throw new TestClustersException("Can not start " + this + ", missing artifact: " + artifact);
        }

        // Note the sync wipes out anything already there
        services.sync(copySpec -> {
            if (getDistribution().getExtension() == "zip") {
                copySpec.from(services.zipTree(artifact));
            } else {
                throw new TestClustersException("Only ZIP distributions are supported for now: " + this);
            }
            copySpec.into(rootDir);
        });

        writeConfigFile();

        startElasticsearchProcess();

        registerCleanupHooks();
    }

    private void startElasticsearchProcess() {
        logger.info("Running `bin/elasticsearch` in `{}` for {}", getWorkDir(), this);
        final ProcessBuilder processBuilder = new ProcessBuilder();
        if (current().isWindows()) {
            processBuilder.command("cmd", "/c", "bin\\elasticsearch.bat");
        } else {
            processBuilder.command("bin/elasticsearch");
        }
        try {
            processBuilder.directory(getWorkDir());
            Map<String, String> environment = processBuilder.environment();
            // Don't inherit anything from the environment for reproducible testing
            environment.clear();
            if (javaHome != null) {
                environment.put("JAVA_HOME", javaHome.getAbsolutePath());
            } else if (System.getenv().get("JAVA_HOME") != null) {
                logger.warn(
                    "{}: No java home configured will use it from environment: {}",
                    this,
                    System.getenv().get("JAVA_HOME")
                );
                environment.put("JAVA_HOME", System.getenv().get("JAVA_HOME"));
            } else {
                logger.warn("{}: No javaHome configured, will rely on java detection", this);
            }
            environment.put("ES_PATH_CONF", getConfigFile().getParentFile().getAbsolutePath());
            environment.put("ES_JAVA_OPTIONS", "-Xms512m -Xmx512m");

            // don't buffer all in memory, make sure we don't block on the default pipes
            processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(getStdErrFile()));
            processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(getStdoutFile()));
            esProcess = processBuilder.start();
        } catch (IOException e) {
            throw new TestClustersException("Failed to start ES process for " + this, e);
        }
    }

    private void registerCleanupHooks() {
        // The Gradle daemon interrupts all threads from the build, so stop the ES process when that happens
        new Thread(
            () -> {
                while (true) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException interrupted) {
                        logger.info("Interrupted, stopping elasticsearch: {}", this);
                        doStop(esProcess.toHandle());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        ).start();
        // When the Daemon is not used, or runs into issues, rely on a shutdown hook
        // When the daemon is used, but does not work correctly and eventually dies off (e.x. due to non interruptable
        // thread in the build) process will be stopped eventually when the daemon dies.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> doStop(esProcess.toHandle())));
    }

    private File getStdoutFile() {
        return new File(getLogsDir(), "es.stdout.log");
    }

    private File getStdErrFile() {
        return new File(getLogsDir(), "es.stderr.log");
    }

    private void writeConfigFile() {
        getConfigFile().getParentFile().mkdirs();
        getConfPathRepo().mkdirs();
        getConfPathData().mkdirs();
        getConfPathSharedData().mkdirs();
        LinkedHashMap<Object, Object> config = new LinkedHashMap<>();
        config.put("cluster.name", "cluster-" + clusterSafeName(name));
        config.put("node.name", "node-" + clusterSafeName(name));
        config.put("path.repo", getConfPathRepo().getAbsolutePath());
        config.put("path.data", getConfPathData().getAbsolutePath());
        config.put("path.shared_data", getConfPathSharedData().getAbsolutePath());
        config.put("node.attr.testattr", "test");
        config.put("node.portsfile", "true");
        config.put("http.port", "0");
        config.put("transport.tcp.port", "0");
        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        config.put("cluster.routing.allocation.disk.watermark.low", "1b");
        config.put("cluster.routing.allocation.disk.watermark.high", "1b");
        // increase script compilation limit since tests can rapid-fire script compilations
        config.put("script.max_compilations_rate", "2048/1m");
        if (version.getMajor() >= 6) {
            config.put("cluster.routing.allocation.disk.watermark.flood_stage", "1b");
        }
        try {
            Files.write(
                getConfigFile().toPath(),
                config.entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n"))
                    .getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            throw new TestClustersException("Could not write config file: " + getConfigFile(), e);
        }
        logger.info("Written config file:{} for {}", getConfigFile(), this);
    }

    private static String clusterSafeName(String name) {
        return name
            .replaceAll("^[:#\\s]+", "")
            .replaceAll("[:#\\s]", "-");
    }

    private void waitForClusterHealthYellow() {
        if (started.get() == false) {
            throw new TestClustersException(
                "`" + this + "` is not started. " +
                    "Elasticsearch is started at execution time automatically when the task calls `useCluster` at " +
                    "configuration time."
            );
        }
        try {
            doWaitClusterHealthYellow();
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for {}", this, e);
            Thread.currentThread().interrupt();
        }
    }

    private void doWaitClusterHealthYellow() throws InterruptedException {
        long startedAt = System.currentTimeMillis();

        waitForCondition(
            startedAt,
            (node -> node.getHttpPortsFile().exists()),
            () -> "waiting for: " + getHttpPortsFile(),
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
        logger.info("{} http is listening on port {}", this, getHttpPortInternal());

        waitForCondition(
            startedAt,
            (node -> node.getTransportPortFile().exists()),
            () -> " waiting for: " + getTransportPortFile(),
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
        logger.info("{} transport is listening on port {}", this, getTransportPortInternal());

        waitForCondition(
            startedAt,
            node -> {
                try {
                    URL url = new URL("http://" +
                        node.getHttpPortInternal().get(0) + // would be nice to have random picking in the build as well
                        "/_cluster/health?wait_for_nodes=>=1&wait_for_status=yellow"
                    );
                    HttpURLConnection con = (HttpURLConnection) url.openConnection();
                    con.setRequestMethod("GET");
                    con.setConnectTimeout(500);
                    con.setReadTimeout(500);
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                        String response = reader.lines().collect(Collectors.joining("\n"));
                        logger.info("{} fully up, responded with:\n{}", this, response);
                    }
                    return true;
                } catch (IOException e) {
                    throw new TestClustersException("Connection attempt to " + this + " failed", e);
                }
            },
            () -> "waiting for cluster health yellow",
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
    }

    private List<String> getTransportPortInternal() {
        try {
            return readPortsFile(getTransportPortFile());
        } catch (IOException e) {
            throw new TestClustersException(
                "Failed to read transport ports file: " + getTransportPortFile() + " for " + this,
                e
            );
        }
    }

    private List<String> readPortsFile(File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            return reader.lines()
                .map(String::trim)
                .collect(Collectors.toList());
        }
    }

    private List<String> getHttpPortInternal() {
        try {
            return readPortsFile(getHttpPortsFile());
        } catch (IOException e) {
            throw new TestClustersException(
                "Failed to read http ports file: " + getHttpPortsFile() + " for " + this ,
                e
            );
        }
    }

    private void waitForCondition(
        long startedAt,
        Predicate<ElasticsearchNode> condition,
        Supplier<String> conditionDescription,
        int totalWaitTimeout, TimeUnit totalWaitTimeoutUnit
    ) throws InterruptedException {
        long thisConditionStartedAt = System.currentTimeMillis();
        boolean conditionMet = false;
        Throwable lastException = null;
        while (System.currentTimeMillis() - startedAt < MILLISECONDS.convert(totalWaitTimeout, totalWaitTimeoutUnit)) {
            if (esProcess.isAlive() == false) {
                throw new TestClustersException(
                    "process was found dead while waiting for " + conditionDescription.get() + ", " + this
                );
            }
            try {
                if (condition.test(this)) {
                    conditionMet = true;
                    break;
                }
            }
            catch (TestClustersException e) {
                if(lastException == null) {
                    lastException = e ;
                } else {
                    e.addSuppressed(lastException);
                    lastException = e;
                }
            }
            logger.debug("{} waiting... {} - {} ({}) < {}",
                this,
                System.currentTimeMillis(), startedAt, System.currentTimeMillis() - startedAt,
                MILLISECONDS.convert(totalWaitTimeout, totalWaitTimeoutUnit)
            );
            Thread.sleep(500);
        }
        if (conditionMet == false) {
            String message = "`" + this + "` failed to start after " +
                totalWaitTimeout + " " + NODE_UP_TIMEOUT_UNIT + ". " + conditionDescription.get();
            if (lastException == null) {
                throw new TestClustersException(message);
            } else {
                throw new TestClustersException(message, lastException);
            }
        }
        logger.info("{}: {} took {} seconds",
            this,
            conditionDescription.get(),
            SECONDS.convert(System.currentTimeMillis() - thisConditionStartedAt, MILLISECONDS)
        );
    }

    /**
     * Stops a running cluster if it's not claimed. Only decrements claims otherwise.
     */
    void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Not stopping `{}`, since node still has {} claim(s)", this , decrementedClaims);
            return;
        }
        if (checkIfCanStopAndMarkStopped() == false) {
            return;
        }
        logger.lifecycle("Stopping `{}`, number of claims is {}", this, decrementedClaims);
        // this can happen if the process doesn't really use the cluster, so a crash would go unnoticed until this point
        // we still want to get diagnostics in this case
        if (esProcess.isAlive() == false) {
            logFileContents("Standard output of node", getStdoutFile());
            logFileContents("Standard error of node", getStdErrFile());
            throw new TestClustersException("Node `" + this + "` wasn't alive when we tried to destroy it");
        }
        doStop(esProcess.toHandle());
        esProcess = null;
    }

    void forceStop() {
        if (checkIfCanStopAndMarkStopped() == false) {
            return;
        }
        logger.lifecycle("Forcefully stopping `{}`, number of claims is {}", this, noOfClaims.get());
        doStop(esProcess.toHandle());
        // Always relay output and error when the task failed
        logFileContents("Standard output of node", getStdoutFile());
        logFileContents("Standard error of node", getStdErrFile());
        esProcess = null;
    }

    private boolean checkIfCanStopAndMarkStopped() {
        // this happens when the process could not start
        if (started.getAndSet(false) == false) {
            logger.lifecycle("`{}` was not running, no need to stop", this);
            return false;
        }
        if (esProcess == null) {
            logger.error("Can't stop node {}. Internal error at startup.", this);
            // don't throw an exception here, as it will mask startup errors, as stop is called even if the start
            // failed Gradle will not show that exception if we throw from here.
            return false;
        }
        return true;
    }

    private static void doStop(ProcessHandle processHandle) {
        // Stop all children first
        if (processHandle.isAlive()) {
            processHandle.children().forEach(ElasticsearchNode::doStop);
        }
        logProcessInfo("Terminating elasticsearch process:", processHandle.info());
        if (processHandle.isAlive()) {
            processHandle.destroy();
        } else {
            logger.info("Process was not running when we tried to terminate it.");
        }
        waitForESProcess(processHandle);
        if (processHandle.isAlive()) {
            logger.info("process did not terminate after {} {}, stopping it forcefully",
                ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT
            );
            processHandle.destroyForcibly();
        }
        waitForESProcess(processHandle);
        if (processHandle.isAlive()) {
            throw new TestClustersException("Was not able to terminate es process");
        }
    }

    private void logFileContents(String description, File from) {
        logger.error("{} `{}`", description, this);
        try (BufferedReader reader = new BufferedReader(new FileReader(from))) {
            reader.lines()
                .map(line -> "    [" + name + "]" + line)
                .forEach(logger::error);
        } catch (IOException e) {
            throw new TestClustersException("Error reading process streams", e);
        }
    }

    private static void logProcessInfo(String prefix, ProcessHandle.Info info) {
        logger.info(prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"), info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[]{}))
                .map(each -> "'" + each + "'")
                .collect(Collectors.joining(", "))
        );
    }

    private static void waitForESProcess(ProcessHandle processHandle) {
        try {
            processHandle.onExit().get(ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for ES process", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.info("Failure while waiting for process to exist", e);
        } catch (TimeoutException e) {
            logger.info("Timed out waiting for process to exit", e);
        }
    }

    private void checkNotRunning() {
        if (started.get()) {
            throw new TestClustersException("Configuration can not be altered while running " + this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchNode that = (ElasticsearchNode) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ElasticsearchNode{" +
            "name='" + name + '\'' +
            ", noOfClaims=" + noOfClaims +
            ", started=" + started +
            '}';
    }
}
