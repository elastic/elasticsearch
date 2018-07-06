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
package org.elasticsearch.gradle.clusterformation;

import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.gradle.Distribution;
import org.elasticsearch.gradle.Version;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.gradle.internal.os.OperatingSystem.current;

public class ElasticsearchNode implements ElasticsearchConfiguration {

    private static final int ES_DESTROY_TIMEOUT = 20;
    private static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int NODE_UP_TIMEOUT = 30;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);

    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final File sharedArtifactsDir;
    private final File syncDir;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private Distribution distribution;
    private Version version;
    private File javaHome;

    private volatile Process esProcess;

    ElasticsearchNode(String name, GradleServicesAdapter services, File sharedArtifactsDir, File workDirBase) {
        this.name = name;
        this.services = services;
        this.sharedArtifactsDir = sharedArtifactsDir;
        this.syncDir = new File(workDirBase, name);
    }

    private File getWorkDir() {
        assertValid();
        return new File(syncDir, getDistribution().getFileName() + "-" + getVersion());
    }

    private File getConfigFile() {
        return new File(getWorkDir(), "config/elasticsearch.yml");
    }

    private File getPathData() {
        return new File(getWorkDir(), "data");
    }

    private File getPathSharedData() {
        return new File(getWorkDir(), "sharedData");
    }

    private File getPathRepo() {
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

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public void setVersion(Version version) {
        checkNotRunning();
        this.version = version;
    }

    @Override
    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public void setDistribution(Distribution distribution) {
        checkNotRunning();
        this.distribution = distribution;
    }

    @Override
    public void setJavaHome(File javaHome) {
        checkNotRunning();
        Objects.requireNonNull(javaHome, "null javaHome passed to cluster formation");
        if (javaHome.exists() == false) {
            throw new ClusterFormationException("java home does not exists `" + javaHome + "`");
        }
        this.javaHome = javaHome;
    }

    @Override
    public File getJavaHome() {
        return javaHome;
    }

    @Override
    public String getHttpSocketURI() {
        waitForClusterHealthYellow();
        return getHttpPortInternal().get(0);
    }

    @Override
    public String getTransportPortURI() {
        waitForClusterHealthYellow();
        return getTransportPortInternal().get(0);
    }

    @Override
    public void claim() {
        noOfClaims.incrementAndGet();
    }

    /**
     * Start the cluster if not running. Does nothing if the cluster is already running.
     */
    @Override
    public void start() {
        if (started.getAndSet(true)) {
            logger.info("Already started `{}`", name);
            return;
        } else {
            logger.lifecycle("Starting `{}`", name);
        }
        File artifact = ClusterformationPlugin.getArtifact(sharedArtifactsDir, getDistribution(), getVersion());
        if (artifact.exists() == false) {
            throw new ClusterFormationException("Can not start node, missing artifact: " + artifact);
        }

        services.sync(copySpec -> {
            if (getDistribution().getExtension() == "zip") {
                copySpec.from(services.zipTree(artifact));
            } else {
                throw new ClusterFormationException("Only ZIP distributions are supported for now");
            }
            copySpec.into(syncDir);
        });

        writeConfigFile();

        logger.info("Running `bin/elasticsearch` in `{}`", getWorkDir());
        if (current().isWindows()) {
            // TODO support windows
            logger.lifecycle("Windows is not supported at this time");
        } else {
            try {
                ProcessBuilder processBuilder = new ProcessBuilder()
                    .command("bin/elasticsearch")
                    .directory(getWorkDir());
                Map<String, String> environment = processBuilder.environment();
                environment.clear();
                if (javaHome != null) {
                    environment.put("JAVA_HOME", javaHome.getAbsolutePath());
                }
                environment.put("ES_PATH_CONF", getConfigFile().getParentFile().getAbsolutePath());
                environment.put("ES_JAVA_OPTIONS", "-Xms512m -Xmx512m");
                // don't buffer all in memory, make sure we don't block on the default pipes
                processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(getStdErrFile()));
                processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(getStdoutFile()));

                esProcess = processBuilder.start();
            } catch (IOException e) {
                throw new ClusterFormationException("Failed to start ES process", e);
            }
        }
    }

    private File getStdoutFile() {
        return new File(getLogsDir(), "es.stdout.log");
    }

    private File getStdErrFile() {
        return new File(getLogsDir(), "es.stderr.log");
    }

    private void writeConfigFile() {
        getConfigFile().getParentFile().mkdirs();
        getPathRepo().mkdirs();
        getPathData().mkdirs();
        getPathSharedData().mkdirs();
        LinkedHashMap<Object, Object> config = new LinkedHashMap<>();
        config.put("cluster.name", "cluster-" + name);
        config.put("node.name", "node-" + name);
        config.put("path.repo", getPathRepo().getAbsolutePath());
        config.put("path.data", getPathData().getAbsolutePath());
        config.put("path.shared_data", getPathSharedData().getAbsolutePath());
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
            Files.write(getConfigFile().toPath(), config.entrySet().stream()
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            throw new ClusterFormationException("Could not write config file: " + getConfigFile(), e);
        }
        logger.info("Written config file :{}", getConfigFile());
    }

    private void waitForClusterHealthYellow() {
        if (started.get() == false) {
            throw new ClusterFormationException(
                "`" + name + "` is not started. Clusters are only started at execution time."
            );
        }
        try {
            doWaitClusterHealthYellow();
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for ES node {}", name, e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new ClusterFormationException("Failed to wait for `" + name + "` to tart up", e);
        }
    }

    private void doWaitClusterHealthYellow() throws InterruptedException, IOException {
        long startedAt = System.currentTimeMillis();

        waitForCondition(
            startedAt,
            (node -> node.getHttpPortsFile().exists()),
            () -> "waiting for: " + getHttpPortsFile(),
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
        logger.info("Node {} http is listening on port {}", name, getHttpPortInternal());

        waitForCondition(
            startedAt,
            (node -> node.getTransportPortFile().exists()),
            () -> " waiting for: " + getTransportPortFile(),
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
        logger.info("Node {} transport is listening on port {}", name, getTransportPortInternal());

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
                        logger.info("Server responded with:\n{}", response);
                        logger.info("`{}` is fully up!", name);
                    }
                    return true;
                } catch (IOException e) {
                    logger.debug("Connection attempt to ES failed", e);
                    return false;
                }
            },
            () -> "waiting for cluster health",
            NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT
        );
    }

    private List<String> getTransportPortInternal() {
        try {
            return readPortsFile(getTransportPortFile());
        } catch (IOException e) {
            throw new ClusterFormationException("Failed to read transport ports file", e);
        }
    }

    private List<String> readPortsFile(File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            return reader.lines()
                .map(String::trim)
                .collect(Collectors.toList());
        }
    }

    private List<String> getHttpPortInternal()  {
        try {
            return readPortsFile(getHttpPortsFile());
        } catch (IOException e) {
            throw new ClusterFormationException("Failed to read http ports file", e);
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
        while (System.currentTimeMillis() - startedAt < TimeUnit.NANOSECONDS.convert(totalWaitTimeout, totalWaitTimeoutUnit)) {
            if (condition.test(this)) {
                conditionMet = true;
                break;
            }
            Thread.sleep(500);
        }
        if (conditionMet == false) {
            throw new ClusterFormationException(
                "Node `" + name + "` failed to start after " +
                    totalWaitTimeout + " " + NODE_UP_TIMEOUT_UNIT + ". " + conditionDescription.get()
            );
        }
        logger.info("{} took {} seconds",
            conditionDescription.get(),
            (System.currentTimeMillis() - thisConditionStartedAt) / 1000.0d
        );
    }

    /**
     * Stops a running cluster if it's not claimed. Does nothing otherwise.
     */
    @Override
    public void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Not stopping `{}`, since node still has {} claim(s)", name, decrementedClaims);
            return;
        }
        if (checkIfCanStopAndMarkStopped() == false) {
            return;
        }
        logger.lifecycle("Stopping `{}`, number of claims is {}", name, decrementedClaims);
        // this can happen if the process doesn't really use the cluster, so a crash would go unnoticed until this point
        // we still want to get streams in this case
        if (esProcess.isAlive() == false) {
            logFileContents("Standard output of node", getStdoutFile());
            logFileContents("Standard error of node", getStdErrFile());
            throw new ClusterFormationException("Node `" + name + "` wasn't alive when we tried to destroy it");
        }
        doStop();
        esProcess = null;
    }

    @Override
    public void forceStop() {
        if (checkIfCanStopAndMarkStopped() == false) {
            return;
        }
        logger.lifecycle("Forcefully stopping `{}`, number of claims is {}", name, noOfClaims.get());
        doStop();
        // Always relay output and error when the task failed
        logFileContents("Standard output of node", getStdoutFile());
        logFileContents("Standard error of node", getStdErrFile());
        esProcess = null;
    }

    private boolean checkIfCanStopAndMarkStopped() {
        // this happens when the process could not start
        if (started.getAndSet(false) == false) {
            logger.lifecycle("`{}` was not running, no need to stop", name);
            return false;
        }
        if (esProcess == null) {
            logger.error("Can't stop node. Internal error at startup.");
            // don't throw an exception here, as it will mask startup errors, as stop is called even if the process was
            // not started.
            return false;
        }
        return true;
    }

    private void doStop() {
        if (current().isWindows()) {
            return;
        }

        logProcessInfo("ES Process:", esProcess.info());
        esProcess.children().forEach(child -> logProcessInfo("Child Process:", child.info()));

        esProcess.destroy();
        waitForESProcess();
        if (esProcess.isAlive()) {
            logger.info("Node `{}` did not terminate after {} {}, stopping it forcefully",
                name, ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT
            );
            esProcess.destroyForcibly();
        }
        waitForESProcess();
        if (esProcess.isAlive()) {
            throw new ClusterFormationException("Was not able to terminate node: " + name);
        }
    }

    private void logFileContents(String description, File from) {
        logger.error("{} `{}`", description, name);
        try (BufferedReader reader = new BufferedReader(new FileReader(from))) {
            reader.lines()
                .map(line -> "    [" + name + "]" + line)
                .forEach(logger::error);
        } catch (IOException e) {
            throw new ClusterFormationException("Error reading process streams", e);
        }
    }

    private void logProcessInfo(String prefix, ProcessHandle.Info info) {
        logger.lifecycle(prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"), info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[]{}))
                .map(each -> "'" + each + "'")
                .collect(Collectors.joining(", "))
        );
    }

    private void waitForESProcess() {
        try {
            esProcess.waitFor(ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for ES process", e);
            Thread.currentThread().interrupt();
        }
    }

    private void checkNotRunning() {
        if (started.get()) {
            throw new IllegalStateException("Configuration can not be altered while running ");
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
}
