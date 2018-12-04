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
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.os.OperatingSystem;

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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchNode {

    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);
    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicBoolean configurationFrozen = new AtomicBoolean(false);
    private final File artifactsExtractDir;
    private final File workingDir;

    private static final int ES_DESTROY_TIMEOUT = 20;
    private static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int NODE_UP_TIMEOUT = 30;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private final LinkedHashMap<String, Predicate<ElasticsearchNode>> waitConditions;

    private Distribution distribution;
    private String version;
    private File javaHome;
    private volatile Process esProcess;
    private final String path;

    ElasticsearchNode(String path, String name, GradleServicesAdapter services, File artifactsExtractDir, File workingDirBase) {
        this.path = path;
        this.name = name;
        this.services = services;
        this.artifactsExtractDir = artifactsExtractDir;
        this.workingDir = new File(workingDirBase, safeName(name));
        this.waitConditions = new LinkedHashMap<>();
        waitConditions.put("http ports file", node -> node.getHttpPortsFile().exists());
        waitConditions.put("transport ports file", node -> node.getTransportPortFile().exists());
        waitForUri("cluster health yellow", "/_cluster/health?wait_for_nodes=>=1&wait_for_status=yellow");
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        checkFrozen();
        this.version = version;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    public void setDistribution(Distribution distribution) {
        requireNonNull(distribution, "null distribution passed when configuring test cluster `" + this + "`");
        checkFrozen();
        this.distribution = distribution;
    }

    public void freeze() {
        requireNonNull(distribution, "null distribution passed when configuring test cluster `" + this + "`");
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        logger.info("Locking configuration of `{}`", this);
        configurationFrozen.set(true);
    }

    public void setJavaHome(File javaHome) {
        requireNonNull(javaHome, "null javaHome passed when configuring test cluster `" + this + "`");
        checkFrozen();
        if (javaHome.exists() == false) {
            throw new TestClustersException("java home for `" + this + "` does not exists: `" + javaHome + "`");
        }
        this.javaHome = javaHome;
    }

    public File getJavaHome() {
        return javaHome;
    }

    private void waitForUri(String description, String uri) {
        waitConditions.put(description, (node) -> {
            try {
                URL url = new URL("http://" + this.getHttpPortInternal().get(0) + uri);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                con.setConnectTimeout(500);
                con.setReadTimeout(500);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                    String response = reader.lines().collect(Collectors.joining("\n"));
                    logger.info("{} -> {} ->\n{}", this, uri, response);
                }
                return true;
            } catch (IOException e) {
                throw new IllegalStateException("Connection attempt to " + this + " failed", e);
            }
        });
    }

    synchronized void start() {
        logger.info("Starting `{}`", this);

        File distroArtifact = new File(
            new File(artifactsExtractDir, distribution.getFileExtension()),
            distribution.getFileName() + "-" + getVersion()
        );
        if (distroArtifact.exists() == false) {
            throw new TestClustersException("Can not start " + this + ", missing: " + distroArtifact);
        }
        if (distroArtifact.isDirectory() == false) {
            throw new TestClustersException("Can not start " + this + ", is not a directory: " + distroArtifact);
        }
        services.sync(spec -> {
            spec.from(new File(distroArtifact, "config"));
            spec.into(getConfigFile().getParent());
        });
        configure();
        startElasticsearchProcess(distroArtifact);
    }

    private void startElasticsearchProcess(File distroArtifact) {
        logger.info("Running `bin/elasticsearch` in `{}` for {}", workingDir, this);
        final ProcessBuilder processBuilder = new ProcessBuilder();
        if (OperatingSystem.current().isWindows()) {
            processBuilder.command(
                "cmd", "/c",
                new File(distroArtifact, "\\bin\\elasticsearch.bat").getAbsolutePath()
            );
        } else {
            processBuilder.command(
                new File(distroArtifact.getAbsolutePath(), "bin/elasticsearch").getAbsolutePath()
            );
        }
        try {
            processBuilder.directory(workingDir);
            Map<String, String> environment = processBuilder.environment();
            // Don't inherit anything from the environment for as that would  lack reproductability
            environment.clear();
            if (javaHome != null) {
                environment.put("JAVA_HOME", getJavaHome().getAbsolutePath());
            } else if (System.getenv().get("JAVA_HOME") != null) {
                logger.warn("{}: No java home configured will use it from environment: {}",
                    this, System.getenv().get("JAVA_HOME")
                );
                environment.put("JAVA_HOME", System.getenv().get("JAVA_HOME"));
            } else {
                logger.warn("{}: No javaHome configured, will rely on default java detection", this);
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

    public String getHttpSocketURI() {
        waitForAllConditions();
        return getHttpPortInternal().get(0);
    }

    public String getTransportPortURI() {
        waitForAllConditions();
        return getTransportPortInternal().get(0);
    }

    synchronized void stop(boolean tailLogs) {
        if (esProcess == null && tailLogs) {
            // This is a special case. If start() throws an exception the plugin will still call stop
            // Another exception here would eat the orriginal.
            return;
        }
        logger.info("Stopping `{}`, tailLogs: {}", this, tailLogs);
        requireNonNull(esProcess, "Can't stop `" + this + "` as it was not started or already stopped.");
        stopHandle(esProcess.toHandle());
        if (tailLogs) {
            logFileContents("Standard output of node", getStdoutFile());
            logFileContents("Standard error of node", getStdErrFile());
        }
        esProcess = null;
    }

    private void stopHandle(ProcessHandle processHandle) {
        // Stop all children first, ES could actually be a child when there's some wrapper process like on Windows.
        if (processHandle.isAlive()) {
            processHandle.children().forEach(this::stopHandle);
        }
        logProcessInfo("Terminating elasticsearch process:", processHandle.info());
        if (processHandle.isAlive()) {
            processHandle.destroy();
        } else {
            logger.info("Process was not running when we tried to terminate it.");
        }
        waitForProcessToExit(processHandle);
        if (processHandle.isAlive()) {
            logger.info("process did not terminate after {} {}, stopping it forcefully",
                ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT
            );
            processHandle.destroyForcibly();
        }
        waitForProcessToExit(processHandle);
        if (processHandle.isAlive()) {
            throw new TestClustersException("Was not able to terminate es process");
        }
    }

    private void logProcessInfo(String prefix, ProcessHandle.Info info) {
        logger.info(prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"), info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[]{}))
                .map(each -> "'" + each + "'")
                .collect(Collectors.joining(" "))
        );
    }

    private void logFileContents(String description, File from) {
        logger.error("{} `{}`", description, this);
        try (BufferedReader reader = new BufferedReader(new FileReader(from))) {
            reader.lines()
                .map(line -> "  [" + name + "]" + line)
                .forEach(logger::error);
        } catch (IOException e) {
            throw new TestClustersException("Error reading " + description, e);
        }
    }

    private void waitForProcessToExit(ProcessHandle processHandle) {
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

    private File getConfigFile() {
        return new File(workingDir, "config/elasticsearch.yml");
    }

    private File getConfPathData() {
        return new File(workingDir, "data");
    }

    private File getConfPathSharedData() {
        return new File(workingDir, "sharedData");
    }

    private File getConfPathRepo() {
        return new File(workingDir, "repo");
    }

    private File getConfPathLogs() {
        return new File(workingDir, "logs");
    }

    private File getStdoutFile() {
        return new File(getConfPathLogs(), "es.stdout.log");
    }

    private File getStdErrFile() {
        return new File(getConfPathLogs(), "es.stderr.log");
    }

    private void configure() {
        getConfigFile().getParentFile().mkdirs();
        getConfPathRepo().mkdirs();
        getConfPathData().mkdirs();
        getConfPathSharedData().mkdirs();
        getConfPathLogs().mkdirs();
        LinkedHashMap<String, String> config = new LinkedHashMap<>();
        config.put("cluster.name", "cluster-" + safeName(name));
        config.put("node.name", "node-" + safeName(name));
        config.put("path.repo", getConfPathRepo().getAbsolutePath());
        config.put("path.data", getConfPathData().getAbsolutePath());
        config.put("path.logs", getConfPathLogs().getAbsolutePath());
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
        if (Version.fromString(version).getMajor() >= 6) {
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

    private void checkFrozen() {
        if (configurationFrozen.get()) {
            throw new IllegalStateException("Configuration can not be altered, already locked");
        }
    }

    private static String safeName(String name) {
        return name
            .replaceAll("^[^a-zA-Z0-9]+", "")
            .replaceAll("[^a-zA-Z0-9]+", "-");
    }

    private File getHttpPortsFile() {
        return new File(getConfPathLogs(), "http.ports");
    }

    private File getTransportPortFile() {
        return new File(getConfPathLogs(), "transport.ports");
    }

    private List<String> getTransportPortInternal() {
        File transportPortFile = getTransportPortFile();
        try {
            return readPortsFile(getTransportPortFile());
        } catch (IOException e) {
            throw new TestClustersException(
                "Failed to read transport ports file: " + transportPortFile + " for " + this, e
            );
        }
    }

    private List<String> getHttpPortInternal() {
        File httpPortsFile = getHttpPortsFile();
        try {
            return readPortsFile(getHttpPortsFile());
        } catch (IOException e) {
            throw new TestClustersException(
                "Failed to read http ports file: " + httpPortsFile + " for " + this, e
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

    private void waitForAllConditions() {
        requireNonNull(esProcess, "Can't wait for `" + this + "` as it was stopped.");
        long startedAt = System.currentTimeMillis();
        logger.info("Starting to wait for cluster to come up");
        waitConditions.forEach((description, predicate) -> {
            long thisConditionStartedAt = System.currentTimeMillis();
            boolean conditionMet = false;
            Throwable lastException = null;
            while (
                System.currentTimeMillis() - startedAt < MILLISECONDS.convert(NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT)
            ) {
                if (esProcess.isAlive() == false) {
                    throw new TestClustersException(
                        "process was found dead while waiting for " + description + ", " + this
                    );
                }
                try {
                    if(predicate.test(this)) {
                        conditionMet = true;
                        break;
                    }
                } catch (TestClustersException e) {
                    throw new TestClustersException(e);
                } catch (Exception e) {
                    if (lastException == null) {
                        lastException = e;
                    } else {
                        e.addSuppressed(lastException);
                        lastException = e;
                    }
                }
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (conditionMet == false) {
                String message = "`" + this + "` failed to wait for " + description + " after " +
                    NODE_UP_TIMEOUT + " " + NODE_UP_TIMEOUT_UNIT;
                if (lastException == null) {
                    throw new TestClustersException(message);
                } else {
                    throw new TestClustersException(message, lastException);
                }
            }
            logger.info(
                "{}: {} took {} seconds",
                this,  description,
                SECONDS.convert(System.currentTimeMillis() - thisConditionStartedAt, MILLISECONDS)
            );
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchNode that = (ElasticsearchNode) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path);
    }

    @Override
    public String toString() {
        return "node{" + path + ":" + name + "}";
    }
}
