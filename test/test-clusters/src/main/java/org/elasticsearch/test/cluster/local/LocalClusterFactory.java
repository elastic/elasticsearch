/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.ClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionDescriptor;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.IOUtils;
import org.elasticsearch.test.cluster.util.OS;
import org.elasticsearch.test.cluster.util.Pair;
import org.elasticsearch.test.cluster.util.ProcessReaper;
import org.elasticsearch.test.cluster.util.ProcessUtils;
import org.elasticsearch.test.cluster.util.Retry;
import org.elasticsearch.test.cluster.util.Version;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.cluster.local.distribution.DistributionType.DEFAULT;
import static org.elasticsearch.test.cluster.util.OS.WINDOWS;

public class LocalClusterFactory implements ClusterFactory<LocalClusterSpec, LocalClusterHandle> {
    private static final Logger LOGGER = LogManager.getLogger(LocalClusterFactory.class);
    private static final Duration NODE_UP_TIMEOUT = Duration.ofMinutes(2);
    private static final Map<Pair<Version, DistributionType>, DistributionDescriptor> TEST_DISTRIBUTIONS = new ConcurrentHashMap<>();
    private static final String TESTS_CLUSTER_MODULES_PATH_SYSPROP = "tests.cluster.modules.path";
    private static final String TESTS_CLUSTER_PLUGINS_PATH_SYSPROP = "tests.cluster.plugins.path";
    private static final String TESTS_CLUSTER_FIPS_JAR_PATH_SYSPROP = "tests.cluster.fips.jars.path";
    private static final String TESTS_CLUSTER_DEBUG_ENABLED_SYSPROP = "tests.cluster.debug.enabled";
    private static final String ENABLE_DEBUG_JVM_ARGS = "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=";
    private static final int DEFAULT_DEBUG_PORT = 5007;

    private final DistributionResolver distributionResolver;

    public LocalClusterFactory(DistributionResolver distributionResolver) {
        this.distributionResolver = distributionResolver;
    }

    @Override
    public LocalClusterHandle create(LocalClusterSpec spec) {
        Path baseWorkingDir;
        try {
            baseWorkingDir = Files.createTempDirectory(spec.getName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return createHandle(baseWorkingDir, spec);
    }

    protected LocalClusterHandle createHandle(Path baseWorkingDir, LocalClusterSpec spec) {
        return new LocalClusterHandle(
            spec.getName(),
            spec.getNodes().stream().map(s -> new Node(baseWorkingDir, distributionResolver, s)).toList()
        );
    }

    public static class Node {
        private final ObjectMapper objectMapper;
        private final Path baseWorkingDir;
        private final DistributionResolver distributionResolver;
        private final LocalNodeSpec spec;
        private final String name;
        private final Path workingDir;
        private final Path repoDir;
        private final Path dataDir;
        private final Path logsDir;
        private final Path configDir;
        private final Path tempDir;

        private Path distributionDir;
        private Version currentVersion;
        private Process process = null;
        private DistributionDescriptor distributionDescriptor;

        public Node(Path baseWorkingDir, DistributionResolver distributionResolver, LocalNodeSpec spec) {
            this(baseWorkingDir, distributionResolver, spec, null);
        }

        public Node(Path baseWorkingDir, DistributionResolver distributionResolver, LocalNodeSpec spec, String suffix) {
            this.objectMapper = new ObjectMapper();
            this.baseWorkingDir = baseWorkingDir;
            this.distributionResolver = distributionResolver;
            this.spec = spec;
            this.name = suffix == null ? spec.getName() : spec.getName() + "-" + suffix;
            this.workingDir = baseWorkingDir.resolve(name);
            this.repoDir = baseWorkingDir.resolve("repo");
            this.dataDir = workingDir.resolve("data");
            this.logsDir = workingDir.resolve("logs");
            this.configDir = workingDir.resolve("config");
            this.tempDir = workingDir.resolve("tmp"); // elasticsearch temporary directory
        }

        public synchronized void start(Version version) {
            LOGGER.info("Starting Elasticsearch node '{}'", name);
            if (version != null) {
                spec.setVersion(version);
            }

            if (currentVersion == null || currentVersion.equals(spec.getVersion()) == false) {
                LOGGER.info("Creating installation for node '{}' in {}", name, workingDir);
                distributionDescriptor = resolveDistribution();
                LOGGER.info("Distribution for node '{}': {}", name, distributionDescriptor);
                initializeWorkingDirectory(currentVersion != null);
                createConfigDirectory();
                copyExtraConfigFiles(); // extra config files might be needed for running cli tools like plugin install
                copyExtraJarFiles();
                installPlugins();
                if (distributionDescriptor.getType() == DistributionType.INTEG_TEST) {
                    installModules();
                }
                currentVersion = spec.getVersion();
            } else {
                createConfigDirectory();
                copyExtraConfigFiles();
            }

            writeConfiguration();
            createKeystore();
            addKeystoreSettings();
            addKeystoreFiles();
            writeSecureSecretsFile();
            configureSecurity();

            startElasticsearch();
        }

        public synchronized void stop(boolean forcibly) {
            LOGGER.info("Shutting down node '{}'", name);
            if (process != null) {
                ProcessUtils.stopHandle(process.toHandle(), forcibly);
                ProcessReaper.instance().unregister(getServiceName());
            }
            deletePortsFiles();
        }

        public void waitForExit() {
            if (process != null) {
                ProcessUtils.waitForExit(process.toHandle());
            }
        }

        public String getHttpAddress() {
            Path portFile = workingDir.resolve("logs").resolve("http.ports");
            if (Files.notExists(portFile)) {
                waitUntilReady();
            }
            return readPortsFile(portFile).get(0);
        }

        public String getTransportEndpoint() {
            Path portsFile = workingDir.resolve("logs").resolve("transport.ports");
            if (Files.notExists(portsFile)) {
                waitUntilReady();
            }
            return readPortsFile(portsFile).get(0);
        }

        public String getRemoteClusterServerEndpoint() {
            Path portsFile = workingDir.resolve("logs").resolve("remote_cluster.ports");
            if (Files.notExists(portsFile)) {
                waitUntilReady();
            }
            return readPortsFile(portsFile).get(0);
        }

        public void deletePortsFiles() {
            try {
                Path hostsFile = workingDir.resolve("config").resolve("unicast_hosts.txt");
                Path httpPortsFile = workingDir.resolve("logs").resolve("http.ports");
                Path transportPortsFile = workingDir.resolve("logs").resolve("transport.ports");
                Path remoteClusterServerPortsFile = workingDir.resolve("logs").resolve("remote_cluster.ports");

                Files.deleteIfExists(hostsFile);
                Files.deleteIfExists(httpPortsFile);
                Files.deleteIfExists(transportPortsFile);
                Files.deleteIfExists(remoteClusterServerPortsFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write unicast_hosts for: " + this, e);
            }
        }

        public String getName() {
            return name;
        }

        public LocalNodeSpec getSpec() {
            return spec;
        }

        Path getWorkingDir() {
            return workingDir;
        }

        public void waitUntilReady() {
            try {
                Retry.retryUntilTrue(NODE_UP_TIMEOUT, Duration.ofMillis(500), () -> {
                    if (process.isAlive() == false) {
                        throw new RuntimeException(
                            "Elasticsearch process died while waiting for ports file. See console output for details."
                        );
                    }

                    Path httpPorts = workingDir.resolve("logs").resolve("http.ports");
                    Path transportPorts = workingDir.resolve("logs").resolve("transport.ports");

                    return Files.exists(httpPorts) && Files.exists(transportPorts);
                });
            } catch (TimeoutException e) {
                throw new RuntimeException("Timed out after " + NODE_UP_TIMEOUT + " waiting for ports files for: " + this);
            } catch (ExecutionException e) {
                throw new RuntimeException("An error occurred while waiting for ports file for: " + this, e);
            }
        }

        private void createConfigDirectory() {
            try {
                IOUtils.deleteWithRetry(configDir);
                Files.createDirectories(configDir);
            } catch (IOException e) {
                throw new UncheckedIOException("An error occurred creating config directory", e);
            }
        }

        private List<String> readPortsFile(Path file) {
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                return lines.map(String::trim).collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to read ports file: " + file, e);
            }
        }

        private void initializeWorkingDirectory(boolean preserveWorkingDirectory) {
            try {
                if (preserveWorkingDirectory == false) {
                    IOUtils.deleteWithRetry(workingDir);
                }

                if (canUseSharedDistribution()) {
                    distributionDir = distributionDescriptor.getDistributionDir();
                } else {
                    distributionDir = OS.conditional(
                        // Use per-version distribution directories on Windows to avoid cleanup failures
                        c -> c.onWindows(() -> workingDir.resolve("distro").resolve(distributionDescriptor.getVersion().toString()))
                            .onUnix(() -> workingDir.resolve("distro"))
                    );

                    if (Files.exists(distributionDir)) {
                        IOUtils.deleteWithRetry(distributionDir);
                    }

                    try {
                        IOUtils.syncWithLinks(distributionDescriptor.getDistributionDir(), distributionDir);
                    } catch (IOUtils.LinkCreationException e) {
                        // Note does not work for network drives, e.g. Vagrant
                        LOGGER.info("Failed to create working dir using hard links. Falling back to copy", e);
                        // ensure we get a clean copy
                        IOUtils.deleteWithRetry(distributionDir);
                        IOUtils.syncWithCopy(distributionDescriptor.getDistributionDir(), distributionDir);
                    }
                }
                Files.createDirectories(repoDir);
                Files.createDirectories(dataDir);
                Files.createDirectories(logsDir);
                Files.createDirectories(tempDir);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create working directory for node '" + name + "'", e);
            }
        }

        /*
         * We can "share" a distribution directory across clusters so long as we aren't modifying it. That means we aren't installing any
         * additional plugins, modules, or jars. This avoids having to copy the test distribution unnecessarily.
         */
        private boolean canUseSharedDistribution() {
            return OS.current() != WINDOWS // Issues with long file paths on Windows in CI
                && System.getProperty(TESTS_CLUSTER_FIPS_JAR_PATH_SYSPROP) == null
                && getSpec().getPlugins().isEmpty()
                && (distributionDescriptor.getType() == DEFAULT || getSpec().getModules().isEmpty());
        }

        private void copyExtraJarFiles() {
            String fipsJarsPath = System.getProperty(TESTS_CLUSTER_FIPS_JAR_PATH_SYSPROP);
            if (fipsJarsPath != null) {
                LOGGER.info("FIPS is enabled. Copying FIPS jars for node: {}", this);
                Path libsDir = distributionDir.resolve("lib");
                Arrays.stream(fipsJarsPath.split(File.pathSeparator)).map(Path::of).forEach(jar -> {
                    LOGGER.info("Copying jar {} to {}", jar, libsDir);
                    try {
                        Files.copy(jar, libsDir.resolve(jar.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new UncheckedIOException("An error occurred copy extra jar files for: " + this, e);
                    }
                });
            }
        }

        private DistributionDescriptor resolveDistribution() {
            return TEST_DISTRIBUTIONS.computeIfAbsent(
                Pair.of(spec.getVersion(), spec.getDistributionType()),
                key -> distributionResolver.resolve(key.left, key.right)
            );
        }

        private void writeConfiguration() {
            Path configFile = configDir.resolve("elasticsearch.yml");
            Path jvmOptionsFile = configDir.resolve("jvm.options");

            try {
                // Write settings to elasticsearch.yml
                Map<String, String> finalSettings = new HashMap<>();
                finalSettings.put("node.name", name);
                finalSettings.put("path.repo", repoDir.toString());
                finalSettings.put("path.data", dataDir.toString());
                finalSettings.put("path.logs", logsDir.toString());
                finalSettings.putAll(spec.resolveSettings());

                Files.writeString(
                    configFile,
                    finalSettings.entrySet()
                        .stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.joining("\n")),
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE
                );

                // Copy additional configuration from distribution
                try (Stream<Path> configFiles = Files.walk(distributionDir.resolve("config"))) {
                    for (Path file : configFiles.toList()) {
                        Path relativePath = distributionDir.resolve("config").relativize(file);
                        Path dest = configDir.resolve(relativePath);
                        if (Files.exists(dest) == false) {
                            Files.createDirectories(dest.getParent());
                            Files.copy(file, dest);
                        }
                    }
                }

                // Patch jvm.options file to update paths
                String content = Files.readString(jvmOptionsFile);
                Map<String, String> expansions = getJvmOptionsReplacements();
                for (String key : expansions.keySet()) {
                    if (content.contains(key) == false) {
                        throw new IOException("Template property '" + key + "' not found in template.");
                    }
                    content = content.replace(key, expansions.get(key));
                }
                Files.writeString(jvmOptionsFile, content);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not write config file: " + configFile, e);
            }
        }

        private void copyExtraConfigFiles() {
            spec.getExtraConfigFiles().forEach((fileName, resource) -> resource.writeTo(configDir.resolve(fileName)));
        }

        private void createKeystore() {
            if (spec.getKeystorePassword() == null || spec.getKeystorePassword().isEmpty()) {
                runToolScript("elasticsearch-keystore", null, "-v", "create");
            } else {
                runToolScript("elasticsearch-keystore", spec.getKeystorePassword() + "\n" + spec.getKeystorePassword(), "create", "-p");
            }
        }

        private void addKeystoreSettings() {
            spec.resolveKeystore().forEach((key, value) -> {
                String input = spec.getKeystorePassword() == null || spec.getKeystorePassword().isEmpty()
                    ? value
                    : spec.getKeystorePassword() + "\n" + value;

                runToolScript("elasticsearch-keystore", input, "add", key);
            });
        }

        private void addKeystoreFiles() {
            spec.getKeystoreFiles().forEach((key, file) -> {
                try {
                    Path path = Files.createTempFile(tempDir, key, null);
                    file.writeTo(path);

                    ProcessUtils.exec(
                        spec.getKeystorePassword(),
                        workingDir,
                        OS.conditional(
                            c -> c.onWindows(() -> distributionDir.resolve("bin").resolve("elasticsearch-keystore.bat"))
                                .onUnix(() -> distributionDir.resolve("bin").resolve("elasticsearch-keystore"))
                        ),
                        getEnvironmentVariables(),
                        false,
                        "add-file",
                        key,
                        path.toString()
                    ).waitFor();
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private void writeSecureSecretsFile() {
            if (spec.getSecrets().isEmpty() == false) {
                try {
                    Path secretsFile = configDir.resolve("secrets/secrets.json");
                    Files.createDirectories(secretsFile.getParent());
                    Map<String, Object> secretsFileContent = new HashMap<>();
                    secretsFileContent.put("secrets", spec.getSecrets());
                    secretsFileContent.put("metadata", Map.of("version", "1", "compatibility", spec.getVersion().toString()));
                    Files.writeString(secretsFile, objectMapper.writeValueAsString(secretsFileContent));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        private void configureSecurity() {
            if (spec.isSecurityEnabled()) {
                if (spec.getUsers().isEmpty() == false) {
                    LOGGER.info("Setting up roles.yml for node '{}'", name);

                    Path destination = workingDir.resolve("config").resolve("roles.yml");
                    spec.getRolesFiles().forEach(rolesFile -> {
                        try (
                            Writer writer = Files.newBufferedWriter(destination, StandardOpenOption.APPEND);
                            Reader reader = new BufferedReader(new InputStreamReader(rolesFile.asStream()))
                        ) {
                            reader.transferTo(writer);
                        } catch (IOException e) {
                            throw new UncheckedIOException("Failed to append roles file " + rolesFile + " to " + destination, e);
                        }
                    });
                }

                LOGGER.info("Creating users for node '{}'", name);
                for (User user : spec.getUsers()) {
                    runToolScript(
                        "elasticsearch-users",
                        null,
                        "useradd",
                        user.getUsername(),
                        "-p",
                        user.getPassword(),
                        "-r",
                        user.getRole()
                    );
                }
            }
        }

        private void installPlugins() {
            if (spec.getPlugins().isEmpty() == false) {
                Pattern pattern = Pattern.compile("(.+)(?:-\\d\\.\\d\\.\\d-SNAPSHOT\\.zip)?");

                LOGGER.info("Installing plugins {} into node '{}", spec.getPlugins(), name);
                List<Path> pluginPaths = Arrays.stream(System.getProperty(TESTS_CLUSTER_PLUGINS_PATH_SYSPROP).split(File.pathSeparator))
                    .map(Path::of)
                    .toList();

                List<String> toInstall = spec.getPlugins()
                    .stream()
                    .map(
                        pluginName -> pluginPaths.stream()
                            .map(path -> Pair.of(pattern.matcher(path.getFileName().toString()), path))
                            .filter(pair -> pair.left.matches())
                            .map(p -> p.right.getParent().resolve(p.left.group(1)))
                            .findFirst()
                            .orElseThrow(() -> {
                                String taskPath = System.getProperty("tests.task");
                                String project = taskPath.substring(0, taskPath.lastIndexOf(':'));

                                throw new RuntimeException(
                                    "Unable to locate plugin '"
                                        + pluginName
                                        + "'. Ensure you've added the following to the build script for project '"
                                        + project
                                        + "':\n\n"
                                        + "dependencies {\n"
                                        + "  clusterPlugins "
                                        + "project(':plugins:"
                                        + pluginName
                                        + "')"
                                        + "\n}"
                                );
                            })
                    )
                    .map(p -> p.toUri().toString())
                    .toList();

                if (spec.getVersion().onOrAfter("7.6.0")) {
                    runToolScript(
                        "elasticsearch-plugin",
                        null,
                        Stream.concat(Stream.of("install", "--batch"), toInstall.stream()).toArray(String[]::new)
                    );
                } else {
                    toInstall.forEach(plugin -> runToolScript("elasticsearch-plugin", null, "install", "--batch", plugin));
                }
            }
        }

        private void installModules() {
            if (spec.getModules().isEmpty() == false) {
                LOGGER.info("Installing modules {} into node '{}", spec.getModules(), name);
                List<Path> modulePaths = Arrays.stream(System.getProperty(TESTS_CLUSTER_MODULES_PATH_SYSPROP).split(File.pathSeparator))
                    .map(Path::of)
                    .toList();

                spec.getModules().forEach(module -> installModule(module, modulePaths));
            }
        }

        private void installModule(String moduleName, List<Path> modulePaths) {
            Path destination = distributionDir.resolve("modules").resolve(moduleName);
            if (Files.notExists(destination)) {
                Path modulePath = modulePaths.stream().filter(path -> path.endsWith(moduleName)).findFirst().orElseThrow(() -> {
                    String taskPath = System.getProperty("tests.task");
                    String project = taskPath.substring(0, taskPath.lastIndexOf(':'));
                    String moduleDependency = moduleName.startsWith("x-pack")
                        ? "project(xpackModule('" + moduleName.substring(7) + "'))"
                        : "project(':modules:" + moduleName + "')";

                    throw new RuntimeException(
                        "Unable to locate module '"
                            + moduleName
                            + "'. Ensure you've added the following to the build script for project '"
                            + project
                            + "':\n\n"
                            + "dependencies {\n"
                            + "  clusterModules "
                            + moduleDependency
                            + "\n}"
                    );

                });

                IOUtils.syncWithCopy(modulePath, destination);

                // Install any extended plugins
                Properties pluginProperties = new Properties();
                try (
                    InputStream in = new BufferedInputStream(
                        new FileInputStream(modulePath.resolve("plugin-descriptor.properties").toFile())
                    )
                ) {
                    pluginProperties.load(in);
                    String extendedProperty = pluginProperties.getProperty("extended.plugins");
                    if (extendedProperty != null) {
                        String[] extendedPlugins = extendedProperty.split(",");
                        for (String plugin : extendedPlugins) {
                            installModule(plugin, modulePaths);
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        private void startElasticsearch() {
            process = ProcessUtils.exec(
                spec.getKeystorePassword() == null ? null : spec.getKeystorePassword() + "\n",
                workingDir,
                OS.conditional(
                    c -> c.onWindows(() -> distributionDir.resolve("bin").resolve("elasticsearch.bat"))
                        .onUnix(() -> distributionDir.resolve("bin").resolve("elasticsearch"))
                ),
                getEnvironmentVariables(),
                true
            );

            ProcessReaper.instance().registerPid(getServiceName(), process.pid());
        }

        private Map<String, String> getEnvironmentVariables() {
            Map<String, String> environment = new HashMap<>(spec.resolveEnvironment());
            environment.put("ES_PATH_CONF", workingDir.resolve("config").toString());
            environment.put("ES_TMPDIR", workingDir.resolve("tmp").toString());
            // Windows requires this as it defaults to `c:\windows` despite ES_TMPDIR
            environment.put("TMP", workingDir.resolve("tmp").toString());

            String featureFlagProperties = "";
            if (spec.getFeatures().isEmpty() == false && distributionDescriptor.isSnapshot() == false) {
                featureFlagProperties = spec.getFeatures()
                    .stream()
                    .filter(f -> spec.getVersion().onOrAfter(f.from) && (f.until == null || spec.getVersion().before(f.until)))
                    .map(f -> "-D" + f.systemProperty)
                    .collect(Collectors.joining(" "));
            }

            String systemProperties = "";
            if (spec.getSystemProperties().isEmpty() == false) {
                systemProperties = spec.getSystemProperties()
                    .entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .map(p -> p.replace("${ES_PATH_CONF}", configDir.toString()))
                    .collect(Collectors.joining(" "));
            }

            String debugArgs = "";
            if (Boolean.getBoolean(TESTS_CLUSTER_DEBUG_ENABLED_SYSPROP)) {
                int port = DEFAULT_DEBUG_PORT + spec.getCluster().getNodes().indexOf(spec);
                debugArgs = ENABLE_DEBUG_JVM_ARGS + port;
            }

            String heapSize = System.getProperty("tests.heap.size", "512m");
            environment.put("ES_JAVA_OPTS", "-Xms" + heapSize + " -Xmx" + heapSize + " -ea -esa "
            // Support passing in additional JVM arguments
                + System.getProperty("tests.jvm.argline", "")
                + " "
                + featureFlagProperties
                + systemProperties
                + debugArgs);

            return environment;
        }

        private Map<String, String> getJvmOptionsReplacements() {
            return Map.of(
                "-XX:HeapDumpPath=data",
                "-XX:HeapDumpPath=" + logsDir,
                "logs/gc.log",
                logsDir.resolve("gc.log").toString(),
                "-XX:ErrorFile=logs/hs_err_pid%p.log",
                "-XX:ErrorFile=" + logsDir.resolve("hs_err_pid%p.log")
            );
        }

        private void runToolScript(String tool, String input, String... args) {
            try {
                int exit = ProcessUtils.exec(
                    input,
                    distributionDir,
                    distributionDir.resolve("bin")
                        .resolve(OS.<String>conditional(c -> c.onWindows(() -> tool + ".bat").onUnix(() -> tool))),
                    getEnvironmentVariables(),
                    false,
                    args
                ).waitFor();

                if (exit != 0) {
                    throw new RuntimeException("Execution of " + tool + " failed with exit code " + exit);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private String getServiceName() {
            return baseWorkingDir.getFileName() + "-" + name;
        }

        @Override
        public String toString() {
            return "{ cluster: '" + spec.getCluster().getName() + "', node: '" + name + "' }";
        }
    }
}
