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
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchNode {

    public static final int CLEAN_WORKDIR_RETRIES = 3;
    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);
    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicBoolean configurationFrozen = new AtomicBoolean(false);
    private final Path artifactsExtractDir;
    private final Path workingDir;

    private final LinkedHashMap<String, Predicate<ElasticsearchNode>> waitConditions;
    private final List<URI> plugins = new ArrayList<>();

    private static final int ES_DESTROY_TIMEOUT = 20;
    private static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int NODE_UP_TIMEOUT = 30;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private final Path confPathRepo;
    private final Path configFile;
    private final Path confPathData;
    private final Path confPathLogs;
    private final Path tmpDir;
    private final Path transportPortFile;
    private final Path httpPortsFile;
    private final Path esStdoutFile;
    private final Path esStderrFile;

    private Distribution distribution;
    private String version;
    private File javaHome;
    private volatile Process esProcess;
    private final String path;

    ElasticsearchNode(String path, String name, GradleServicesAdapter services, File artifactsExtractDir, File workingDirBase) {
        this.path = path;
        this.name = name;
        this.services = services;
        this.artifactsExtractDir = artifactsExtractDir.toPath();
        this.workingDir = workingDirBase.toPath().resolve(safeName(name)).toAbsolutePath();
        confPathRepo = workingDir.resolve("repo");
        configFile = workingDir.resolve("config/elasticsearch.yml");
        confPathData = workingDir.resolve("data");
        confPathLogs = workingDir.resolve("logs");
        tmpDir = workingDir.resolve("tmp");
        transportPortFile = confPathLogs.resolve("transport.ports");
        httpPortsFile = confPathLogs.resolve("http.ports");
        this.waitConditions = new LinkedHashMap<>();
        waitConditions.put("http ports file", node -> Files.exists(node.httpPortsFile));
        waitConditions.put("transport ports file", node -> Files.exists(node.transportPortFile));
        waitForUri("cluster health yellow", "/_cluster/health?wait_for_nodes=>=1&wait_for_status=yellow");
        esStdoutFile = confPathLogs.resolve("es.stdout.log");
        esStderrFile = confPathLogs.resolve("es.stderr.log");
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

    public void plugin(URI plugin) {
        requireNonNull(plugin, "Plugin name can't be null");
        checkFrozen();
        this.plugins.add(plugin);
    }

    public void plugin(String plugin) {
        plugin(URI.create(plugin));
    }

    public void plugin(File plugin) {
        plugin(plugin.toURI());
    }

    public void freeze() {
        requireNonNull(distribution, "null distribution passed when configuring test cluster `" + this + "`");
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        requireNonNull(javaHome, "null javaHome passed when configuring test cluster `" + this + "`");
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
        logger.info("Java home is {}, configuration is frozen: ", javaHome, configurationFrozen.get());
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
                throw new UncheckedIOException("Connection attempt to " + this + " failed", e);
            }
        });
    }

    synchronized void start() {
        logger.info("Starting `{}`", this);

        Path distroExtractDir = artifactsExtractDir
                .resolve(distribution.getFileExtension())
                .resolve(distribution.getFileName() + "-" + getVersion());

        if (Files.isDirectory(distroExtractDir) == false) {
            if (Files.exists(distroExtractDir) == false) {
                throw new TestClustersException("Can not start " + this + ", missing: " + distroExtractDir);
            }
            throw new TestClustersException("Can not start " + this + ", is not a directory: " + distroExtractDir);
        }

        createWorkingDir(distroExtractDir);

        createConfiguration();

        plugins.forEach(plugin -> runElaticsearchBinScript(
            "elasticsearch-plugin",
            "install", "--batch", plugin.toString())
        );

        startElasticsearchProcess();
    }

    private void runElaticsearchBinScript(String tool, String... args) {
        services.loggedExec(spec -> {
            spec.setEnvironment(getESEnvironment());
            spec.workingDir(workingDir);
            if (OperatingSystem.current().isWindows()) {
                spec.executable("cmd");
                spec.args("/c");
                spec.args("bin\\" + tool + ".bat");
                spec.args((Object[]) args);
            } else {
                spec.executable("./bin/" + tool);
                spec.args((Object[]) args);
            }
        });
    }

    private void startElasticsearchProcess() {
        logger.info("Running `bin/elasticsearch` in `{}` for {}", workingDir, this);
        final ProcessBuilder processBuilder = new ProcessBuilder();
        if (OperatingSystem.current().isWindows()) {
            processBuilder.command(
                "cmd", "/c",
                "bin\\elasticsearch.bat"
            );
        } else {
            processBuilder.command(
                "./bin/elasticsearch"
            );
        }
        try {
            processBuilder.directory(workingDir.toFile());
            Map<String, String> environment = processBuilder.environment();
            // Don't inherit anything from the environment for as that would  lack reproductability
            environment.clear();
            environment.putAll(getESEnvironment());
            // don't buffer all in memory, make sure we don't block on the default pipes
            processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(esStderrFile.toFile()));
            processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(esStdoutFile.toFile()));
            esProcess = processBuilder.start();
        } catch (IOException e) {
            throw new TestClustersException("Failed to start ES process for " + this, e);
        }
    }

    private Map<String, String> getESEnvironment() {
        Map<String, String> environment= new HashMap<>();
        environment.put("JAVA_HOME", getJavaHome().getAbsolutePath());
        environment.put("ES_PATH_CONF", configFile.getParent().toString());
        environment.put("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        environment.put("ES_TMPDIR", tmpDir.toString());
        // Windows requires this as it defaults to `c:\windows` despite ES_TMPDIR

        environment.put("TMP", tmpDir.toString());
        return environment;
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
            logFileContents("Standard output of node", esStdoutFile);
            logFileContents("Standard error of node", esStderrFile);
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

    private void logFileContents(String description, Path from) {
        logger.error("{} `{}`", description, this);
        try (BufferedReader reader = new BufferedReader(new FileReader(from.toFile()))) {
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

    private void createWorkingDir(Path distroExtractDir) {
        try {
            syncWithLinks(distroExtractDir, workingDir);
            Files.createDirectories(confPathRepo);
            Files.createDirectories(confPathData);
            Files.createDirectories(workingDir.resolve("sharedData"));
            Files.createDirectories(confPathLogs);
            Files.createDirectories(tmpDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create working directory: " + workingDir, e);
        }
    }

    public void syncWithLinks(Path sourceRoot, Path destinationRoot) throws IOException {
        // There's some latency in Windows between when a cluster running here previously releases the files so we can
        // can clean them up. Make sure we can run the same clusters in quick succession.
        removeWithRetry(destinationRoot);
        try (Stream<Path> stream = Files.walk(sourceRoot)) {
            stream.forEach(source -> {
                Path destination = destinationRoot.resolve(sourceRoot.relativize(source));
                if (Files.isDirectory(source)) {
                    try {
                        Files.createDirectories(destination);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Can't create directory " + destination.getParent(), e);
                    }
                } else {
                    try {
                        Files.createDirectories(destination.getParent());
                    } catch (IOException e) {
                        throw new UncheckedIOException("Can't create directory " + destination.getParent(), e);
                    }
                    try {
                        Files.createLink(destination, source);
                    } catch (IOException e) {
                        // Note does not work for network drives, e.x. Vagrant
                        throw new UncheckedIOException(
                            "Failed to create hard link " + destination + " pointing to " + source, e
                        );
                    }
                    try {
                        setPermissionsReadOnly(source, destination);
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                            "Failed to set permissions " + destination + " pointing to " + source, e
                        );
                    }
                }
            });
        }
    }

    private void removeWithRetry(Path destinationRoot) {
        if (Files.exists(destinationRoot)) {
            for (int tries = 1; true; tries++) {
                try (Stream<Path> stream = Files.walk(destinationRoot)) {
                    stream.sorted(Comparator.reverseOrder()).forEach(toDelete -> {
                        try {
                            Files.delete(toDelete);
                        } catch (IOException e) {
                            throw new UncheckedIOException("Can't remove " + toDelete, e);
                        }
                    });
                    return ;
                } catch (UncheckedIOException e) {
                    if (tries == CLEAN_WORKDIR_RETRIES) {
                        throw e;
                    } else {
                        logger.info("Try {}/{} to remove {} failed, will retry", tries, CLEAN_WORKDIR_RETRIES, destinationRoot, e);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                try {
                    Thread.sleep(SECONDS.toMillis(2));
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for cleanup", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void setPermissionsReadOnly(Path source, Path destination) throws IOException {
        // Since we create hard links, if anyone writes to any of the destinations it will reflect all across, we need
        // to remove write permissions to prevent this.
        Set<String> attributeViews = destination.getFileSystem().supportedFileAttributeViews();
        if (destination.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            Set<PosixFilePermission> permissions = new HashSet<>(Files.getPosixFilePermissions(source));
            permissions.remove(PosixFilePermission.OWNER_WRITE);
            permissions.remove(PosixFilePermission.GROUP_WRITE);
            permissions.remove(PosixFilePermission.OTHERS_WRITE);
            Files.setPosixFilePermissions(destination, permissions);
        } else if (attributeViews.contains("acl")) {
            AclFileAttributeView view = Files.getFileAttributeView(destination, AclFileAttributeView.class);
            List<AclEntry> entries = new ArrayList<>();
            for (AclEntry acl : view.getAcl()) {
                Set<AclEntryPermission> perms = new LinkedHashSet<>(acl.permissions());
                perms.remove(AclEntryPermission.WRITE_DATA);
                perms.remove(AclEntryPermission.APPEND_DATA);
                entries.add(
                    AclEntry.newBuilder()
                        .setType(acl.type())
                        .setPrincipal(acl.principal())
                        .setPermissions(perms)
                        .setFlags(acl.flags())
                        .build()
                );
            }
            view.setAcl(entries);
        } else {
            throw new TestClustersException("Unsupported file attribute view: " + attributeViews);
        }
    }

    private void createConfiguration() {
        LinkedHashMap<String, String> config = new LinkedHashMap<>();
        config.put("cluster.name", "cluster-" + safeName(name));
        config.put("node.name", "node-" + safeName(name));
        config.put("path.home", workingDir.toString());
        config.put("path.repo", confPathRepo.toString());
        config.put("path.data", confPathData.toString());
        config.put("path.logs", confPathLogs.toString());
        config.put("path.shared_data", workingDir.resolve("sharedData").toString());
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
            // This will be a link to the configuration from the distribution
            Files.delete(configFile);
            Files.write(
                configFile,
                config.entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n"))
                    .getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write config file: " + configFile, e);
        }
        logger.info("Written config file:{} for {}", configFile, this);
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

    private List<String> getTransportPortInternal() {
        try {
            return readPortsFile(transportPortFile);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to read transport ports file: " + transportPortFile + " for " + this, e
            );
        }
    }

    private List<String> getHttpPortInternal() {
        try {
            return readPortsFile(httpPortsFile);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to read http ports file: " + httpPortsFile + " for " + this, e
            );
        }
    }

    private List<String> readPortsFile(Path file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
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
                    if (predicate.test(this)) {
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
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (conditionMet == false) {
                logger.error("Wait for cluster timed out: {}", this);
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
                this, description,
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
