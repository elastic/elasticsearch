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
import org.elasticsearch.gradle.FileSupplier;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.http.WaitForHttpResource;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class ElasticsearchNode implements TestClusterConfiguration {

    private static final Logger LOGGER = Logging.getLogger(ElasticsearchNode.class);
    private static final int ES_DESTROY_TIMEOUT = 20;
    private static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int NODE_UP_TIMEOUT = 60;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final List<String> OVERRIDABLE_SETTINGS = Arrays.asList(
        "path.repo",
        "discovery.seed_providers"
    );

    private final String path;
    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicBoolean configurationFrozen = new AtomicBoolean(false);
    private final Path artifactsExtractDir;
    private final Path workingDir;


    private final LinkedHashMap<String, Predicate<TestClusterConfiguration>> waitConditions = new LinkedHashMap<>();
    private final List<URI> plugins = new ArrayList<>();
    private final List<File> modules = new ArrayList<>();
    private final Map<String, Supplier<CharSequence>> settings = new LinkedHashMap<>();
    private final Map<String, Supplier<CharSequence>> keystoreSettings = new LinkedHashMap<>();
    private final Map<String, FileSupplier> keystoreFiles = new LinkedHashMap<>();
    private final Map<String, Supplier<CharSequence>> systemProperties = new LinkedHashMap<>();
    private final Map<String, Supplier<CharSequence>> environment = new LinkedHashMap<>();
    private final List<Supplier<List<CharSequence>>> jvmArgs = new ArrayList<>();
    private final Map<String, File> extraConfigFiles = new HashMap<>();
    final LinkedHashMap<String, String> defaultConfig = new LinkedHashMap<>();
    private final List<Map<String, String>> credentials = new ArrayList<>();

    private final Path confPathRepo;
    private final Path configFile;
    private final Path confPathData;
    private final Path confPathLogs;
    private final Path transportPortFile;
    private final Path httpPortsFile;
    private final Path esStdoutFile;
    private final Path esStderrFile;
    private final Path tmpDir;

    private Distribution distribution;
    private String version;
    private File javaHome;
    private volatile Process esProcess;
    private Function<String, String> nameCustomization = Function.identity();
    private boolean isWorkingDirConfigured = false;

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
        transportPortFile = confPathLogs.resolve("transport.ports");
        httpPortsFile = confPathLogs.resolve("http.ports");
        esStdoutFile = confPathLogs.resolve("es.stdout.log");
        esStderrFile = confPathLogs.resolve("es.stderr.log");
        tmpDir = workingDir.resolve("tmp");
        waitConditions.put("ports files", this::checkPortsFilesExistWithDelay);
    }

    public String getName() {
        return nameCustomization.apply(name);
    }

    public String getVersion() {
        return version;
    }

    @Override
    public void setVersion(String version) {
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        checkFrozen();
        this.version = version;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public void setDistribution(Distribution distribution) {
        requireNonNull(distribution, "null distribution passed when configuring test cluster `" + this + "`");
        checkFrozen();
        this.distribution = distribution;
    }

    @Override
    public void plugin(URI plugin) {
        requireNonNull(plugin, "Plugin name can't be null");
        checkFrozen();
        this.plugins.add(plugin);
    }

    @Override
    public void plugin(File plugin) {
        plugin(plugin.toURI());
    }

    @Override
    public void module(File module) {
        this.modules.add(module);
    }

    @Override
    public void keystore(String key, String value) {
        addSupplier("Keystore", keystoreSettings, key, value);
    }

    @Override
    public void keystore(String key, Supplier<CharSequence> valueSupplier) {
        addSupplier("Keystore", keystoreSettings, key, valueSupplier);
    }

    @Override
    public void keystore(String key, File value) {
        requireNonNull(value, "keystore value was null when configuring test cluster`" + this + "`");
        keystore(key, () -> value);
    }

    @Override
    public void keystore(String key, FileSupplier valueSupplier) {
        requireNonNull(key, "Keystore" + " key was null when configuring test cluster `" + this + "`");
        requireNonNull(valueSupplier, "Keystore" + " value supplier was null when configuring test cluster `" + this + "`");
        keystoreFiles.put(key, valueSupplier);
    }

    @Override
    public void setting(String key, String value) {
        addSupplier("Settings", settings, key, value);
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier) {
        addSupplier("Setting", settings, key, valueSupplier);
    }

    @Override
    public void systemProperty(String key, String value) {
        addSupplier("Java System property", systemProperties, key, value);
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier) {
        addSupplier("Java System property", systemProperties, key, valueSupplier);
    }

    @Override
    public void environment(String key, String value) {
        addSupplier("Environment variable", environment, key, value);
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier) {
        addSupplier("Environment variable", environment, key, valueSupplier);
    }


    public void jvmArgs(String... values) {
        for (String value : values) {
            requireNonNull(value, "jvm argument was null when configuring test cluster `" + this + "`");
        }
        jvmArgs.add(() -> Arrays.asList(values));
    }

    public void jvmArgs(Supplier<String[]> valueSupplier) {
        requireNonNull(valueSupplier, "jvm argument supplier was null when configuring test cluster `" + this + "`");
        jvmArgs.add(() -> Arrays.asList(valueSupplier.get()));
    }

    private void addSupplier(String name, Map<String, Supplier<CharSequence>> collector, String key, Supplier<CharSequence> valueSupplier) {
        requireNonNull(key, name + " key was null when configuring test cluster `" + this + "`");
        requireNonNull(valueSupplier, name + " value supplier was null when configuring test cluster `" + this + "`");
        collector.put(key, valueSupplier);
    }

    private void addSupplier(String name, Map<String, Supplier<CharSequence>> collector, String key, String actualValue) {
        requireNonNull(actualValue, name + " value was null when configuring test cluster `" + this + "`");
        addSupplier(name, collector, key, () -> actualValue);
    }

    private void checkSuppliers(String name, Collection<Supplier<CharSequence>> collector) {
        collector.forEach(suplier ->
            requireNonNull(
                suplier.get().toString(),
                name + " supplied value was null when configuring test cluster `" + this + "`"
            )
        );
    }

    public Path getConfigDir() {
        return configFile.getParent();
    }

    @Override
    public void freeze() {
        requireNonNull(distribution, "null distribution passed when configuring test cluster `" + this + "`");
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        requireNonNull(javaHome, "null javaHome passed when configuring test cluster `" + this + "`");
        LOGGER.info("Locking configuration of `{}`", this);
        configurationFrozen.set(true);
    }

    @Override
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

    /**
     * Returns a stream of lines in the generated logs similar to Files.lines
     *
     * @return stream of log lines
     */
    public Stream<String> logLines() throws IOException {
        return Files.lines(esStdoutFile, StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void start() {
        LOGGER.info("Starting `{}`", this);

        Path distroArtifact = artifactsExtractDir
            .resolve(distribution.getGroup())
            .resolve("elasticsearch-" + getVersion());

        if (Files.exists(distroArtifact) == false) {
            throw new TestClustersException("Can not start " + this + ", missing: " + distroArtifact);
        }
        if (Files.isDirectory(distroArtifact) == false) {
            throw new TestClustersException("Can not start " + this + ", is not a directory: " + distroArtifact);
        }

        try {
            if (isWorkingDirConfigured == false) {
                // Only configure working dir once so we don't loose data on restarts
                isWorkingDirConfigured = true;
                createWorkingDir(distroArtifact);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create working directory for " + this, e);
        }
        createConfiguration();

        plugins.forEach(plugin -> runElaticsearchBinScript(
            "elasticsearch-plugin",
            "install", "--batch", plugin.toString())
        );

        if (keystoreSettings.isEmpty() == false || keystoreFiles.isEmpty() == false) {
            runElaticsearchBinScript("elasticsearch-keystore", "create");

            checkSuppliers("Keystore", keystoreSettings.values());
            keystoreSettings.forEach((key, value) ->
                runElaticsearchBinScriptWithInput(value.get().toString(), "elasticsearch-keystore", "add", "-x", key)
            );

            for (Map.Entry<String, FileSupplier> entry : keystoreFiles.entrySet()) {
                File file = entry.getValue().get();
                requireNonNull(file, "supplied keystoreFile was null when configuring " + this);
                if (file.exists() == false) {
                    throw new TestClustersException("supplied keystore file " + file + " does not exist, require for " + this);
                }
                runElaticsearchBinScript("elasticsearch-keystore", "add-file", entry.getKey(), file.getAbsolutePath());
            }
        }

        installModules();

        copyExtraConfigFiles();

        if (isSettingMissingOrTrue("xpack.security.enabled")) {
            if (credentials.isEmpty()) {
                user(Collections.emptyMap());
            }
            credentials.forEach(paramMap -> runElaticsearchBinScript(
                "elasticsearch-users",
                    paramMap.entrySet().stream()
                        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                        .toArray(String[]::new)
            ));
        }

        startElasticsearchProcess();
    }

    @Override
    public void restart() {
        LOGGER.info("Restarting {}", this);
        stop(false);
        try {
            Files.delete(httpPortsFile);
            Files.delete(transportPortFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        start();
    }

    private boolean isSettingMissingOrTrue(String name) {
        return Boolean.valueOf(settings.getOrDefault(name, () -> "false").get().toString());
    }

    private void copyExtraConfigFiles() {
        extraConfigFiles.forEach((destination, from) -> {
                if (Files.exists(from.toPath()) == false) {
                    throw new TestClustersException("Can't create extra config file from " + from + " for " + this +
                        " as it does not exist");
                }
                Path dst = configFile.getParent().resolve(destination);
                try {
                    Files.createDirectories(dst.getParent());
                    Files.copy(from.toPath(), dst, StandardCopyOption.REPLACE_EXISTING);
                    LOGGER.info("Added extra config file {} for {}", destination, this);
                } catch (IOException e) {
                    throw new UncheckedIOException("Can't create extra config file for", e);
                }
            });
    }

    private void installModules() {
        if (distribution == Distribution.INTEG_TEST) {
            for (File module : modules) {
                Path destination = workingDir.resolve("modules").resolve(module.getName().replace(".zip", "").replace("-" + version, ""));

                // only install modules that are not already bundled with the integ-test distribution
                if (Files.exists(destination) == false) {
                    services.copy(spec -> {
                        if (module.getName().toLowerCase().endsWith(".zip")) {
                            spec.from(services.zipTree(module));
                        } else if (module.isDirectory()) {
                            spec.from(module);
                        } else {
                            throw new IllegalArgumentException("Not a valid module " + module + " for " + this);
                        }
                        spec.into(destination);
                    });
                }
            }
        } else {
            LOGGER.info("Not installing " + modules.size() + "(s) since the " + distribution + " distribution already " +
                "has them");
        }
    }

    @Override
    public void extraConfigFile(String destination, File from) {
        if (destination.contains("..")) {
            throw new IllegalArgumentException("extra config file destination can't be relative, was " + destination +
                " for " + this);
        }
        extraConfigFiles.put(destination, from);
    }

    @Override
    public void user(Map<String, String> userSpec) {
        Set<String> keys = new HashSet<>(userSpec.keySet());
        keys.remove("username");
        keys.remove("password");
        keys.remove("role");
        if (keys.isEmpty() == false) {
            throw new TestClustersException("Unknown keys in user definition " + keys + " for " + this);
        }
        Map<String,String> cred = new LinkedHashMap<>();
        cred.put("useradd", userSpec.getOrDefault("username","test_user"));
        cred.put("-p", userSpec.getOrDefault("password","x-pack-test-password"));
        cred.put("-r", userSpec.getOrDefault("role", "superuser"));
        credentials.add(cred);
    }

    private void runElaticsearchBinScriptWithInput(String input, String tool, String... args) {
        if (
            Files.exists(workingDir.resolve("bin").resolve(tool)) == false &&
                Files.exists(workingDir.resolve("bin").resolve(tool + ".bat")) == false
        ) {
            throw new TestClustersException("Can't run bin script: `" + tool + "` does not exist. " +
                "Is this the distribution you expect it to be ?");
        }
        try (InputStream byteArrayInputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))) {
            services.loggedExec(spec -> {
                spec.setEnvironment(getESEnvironment());
                spec.workingDir(workingDir);
                spec.executable(
                    OS.conditionalString()
                        .onUnix(() -> "./bin/" + tool)
                        .onWindows(() -> "cmd")
                        .supply()
                );
                spec.args(
                    OS.<List<String>>conditional()
                        .onWindows(() -> {
                            ArrayList<String> result = new ArrayList<>();
                            result.add("/c");
                            result.add("bin\\" + tool + ".bat");
                            for (String arg : args) {
                                result.add(arg);
                            }
                            return result;
                        })
                        .onUnix(() -> Arrays.asList(args))
                        .supply()
                );
                spec.setStandardInput(byteArrayInputStream);

            });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to run " + tool + " for " + this, e);
        }
    }

    private void runElaticsearchBinScript(String tool, String... args) {
        runElaticsearchBinScriptWithInput("", tool, args);
    }

    private Map<String, String> getESEnvironment() {
        Map<String, String> defaultEnv = new HashMap<>();
        defaultEnv.put("JAVA_HOME", getJavaHome().getAbsolutePath());
        defaultEnv.put("ES_PATH_CONF", configFile.getParent().toString());
        String systemPropertiesString = "";
        if (systemProperties.isEmpty() == false) {
            checkSuppliers("Java System property", systemProperties.values());
            systemPropertiesString = " " + systemProperties.entrySet().stream()
                .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue().get())
                .collect(Collectors.joining(" "));
        }
        String jvmArgsString = "";
        if (jvmArgs.isEmpty() == false) {
            jvmArgsString = " " + jvmArgs.stream()
                .map(Supplier::get)
                .peek(charSequences -> requireNonNull(charSequences, "Jvm argument supplier returned null while configuring " + this))
                .flatMap(Collection::stream)
                .peek(argument -> {
                    requireNonNull(argument, "Jvm argument supplier returned null while configuring " + this);
                    if (argument.toString().startsWith("-D")) {
                        throw new TestClustersException("Invalid jvm argument `" + argument +
                            "` configure as systemProperty instead for " + this
                        );
                    }
                })
                .collect(Collectors.joining(" "));
        }
        defaultEnv.put("ES_JAVA_OPTS", "-Xms512m -Xmx512m -ea -esa" +
            systemPropertiesString + jvmArgsString
        );
        defaultEnv.put("ES_TMPDIR", tmpDir.toString());
        // Windows requires this as it defaults to `c:\windows` despite ES_TMPDIR
        defaultEnv.put("TMP", tmpDir.toString());

        Set<String> commonKeys = new HashSet<>(environment.keySet());
        commonKeys.retainAll(defaultEnv.keySet());
        if (commonKeys.isEmpty() == false) {
            throw new IllegalStateException(
                "testcluster does not allow overwriting the following env vars " + commonKeys + " for " + this
            );
        }

        checkSuppliers("Environment variable", environment.values());
        environment.forEach((key, value) -> defaultEnv.put(key, value.get().toString()));
        return defaultEnv;
    }

    private void startElasticsearchProcess() {
        final ProcessBuilder processBuilder = new ProcessBuilder();

        List<String> command = OS.<List<String>>conditional()
            .onUnix(() -> Arrays.asList("./bin/elasticsearch"))
            .onWindows(() -> Arrays.asList("cmd", "/c", "bin\\elasticsearch.bat"))
            .supply();
        processBuilder.command(command);
        processBuilder.directory(workingDir.toFile());
        Map<String, String> environment = processBuilder.environment();
        // Don't inherit anything from the environment for as that would  lack reproducibility
        environment.clear();
        environment.putAll(getESEnvironment());
        // don't buffer all in memory, make sure we don't block on the default pipes
        processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(esStderrFile.toFile()));
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(esStdoutFile.toFile()));
        LOGGER.info("Running `{}` in `{}` for {} env: {}", command, workingDir, this, environment);
        try {
            esProcess = processBuilder.start();
        } catch (IOException e) {
            throw new TestClustersException("Failed to start ES process for " + this, e);
        }
    }

    @Override
    public String getHttpSocketURI() {
        return getHttpPortInternal().get(0);
    }

    @Override
    public String getTransportPortURI() {
        return getTransportPortInternal().get(0);
    }

    @Override
    public List<String> getAllHttpSocketURI() {
        return getHttpPortInternal();
    }

    @Override
    public List<String> getAllTransportPortURI() {
        return getTransportPortInternal();
    }

    public File getServerLog() {
        return confPathLogs.resolve(defaultConfig.get("cluster.name") + "_server.json").toFile();
    }

    public File getAuditLog() {
        return confPathLogs.resolve(defaultConfig.get("cluster.name") + "_audit.json").toFile();
    }

    @Override
    public synchronized void stop(boolean tailLogs) {
        if (esProcess == null && tailLogs) {
            // This is a special case. If start() throws an exception the plugin will still call stop
            // Another exception here would eat the orriginal.
            return;
        }
        LOGGER.info("Stopping `{}`, tailLogs: {}", this, tailLogs);
        requireNonNull(esProcess, "Can't stop `" + this + "` as it was not started or already stopped.");
        // Test clusters are not reused, don't spend time on a graceful shutdown
        stopHandle(esProcess.toHandle(), true);
        if (tailLogs) {
            logFileContents("Standard output of node", esStdoutFile);
            logFileContents("Standard error of node", esStderrFile);
        }
        esProcess = null;
    }

    @Override
    public void setNameCustomization(Function<String, String> nameCustomizer) {
        this.nameCustomization = nameCustomizer;
    }

    private void stopHandle(ProcessHandle processHandle, boolean forcibly) {
        // Stop all children first, ES could actually be a child when there's some wrapper process like on Windows.
        if (processHandle.isAlive() == false) {
            LOGGER.info("Process was not running when we tried to terminate it.");
            return;
        }

        // Stop all children first, ES could actually be a child when there's some wrapper process like on Windows.
        processHandle.children().forEach(each -> stopHandle(each, forcibly));

        logProcessInfo(
            "Terminating elasticsearch process" + (forcibly ? " forcibly " : "gracefully") + ":",
            processHandle.info()
        );

        if (forcibly) {
            processHandle.destroyForcibly();
        } else {
            processHandle.destroy();
            waitForProcessToExit(processHandle);
            if (processHandle.isAlive() == false) {
                return;
            }
            LOGGER.info("process did not terminate after {} {}, stopping it forcefully",
                ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT);
            processHandle.destroyForcibly();
        }

        waitForProcessToExit(processHandle);
        if (processHandle.isAlive()) {
            throw new TestClustersException("Was not able to terminate elasticsearch process for " + this);
        }
    }

    private void logProcessInfo(String prefix, ProcessHandle.Info info) {
        LOGGER.info(prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"), info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[]{}))
                .map(each -> "'" + each + "'")
                .collect(Collectors.joining(" "))
        );
    }

    private void logFileContents(String description, Path from) {
        LOGGER.error("{} `{}`", description, this);
        try(Stream<String> lines = Files.lines(from, StandardCharsets.UTF_8)) {
            lines
                .map(line -> "  " + line)
                .forEach(LOGGER::error);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to tail log " + this, e);
        }
    }

    private void waitForProcessToExit(ProcessHandle processHandle) {
        try {
            processHandle.onExit().get(ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for ES process", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.info("Failure while waiting for process to exist", e);
        } catch (TimeoutException e) {
            LOGGER.info("Timed out waiting for process to exit", e);
        }
    }

    private void createWorkingDir(Path distroExtractDir) throws IOException {
        syncWithLinks(distroExtractDir, workingDir);
        Files.createDirectories(configFile.getParent());
        Files.createDirectories(confPathRepo);
        Files.createDirectories(confPathData);
        Files.createDirectories(confPathLogs);
        Files.createDirectories(tmpDir);
    }

    /**
     * Does the equivalent of `cp -lr` and `chmod -r a-w` to save space and improve speed.
     * We remove write permissions to make sure files are note mistakenly edited ( e.x. the config file ) and changes
     * reflected across all copies. Permissions are retained to be able to replace the links.
     *
     * @param sourceRoot where to copy from
     * @param destinationRoot destination to link to
     */
    private void syncWithLinks(Path sourceRoot, Path destinationRoot) {
        if (Files.exists(destinationRoot)) {
            services.delete(destinationRoot);
        }

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
                        // Note does not work for network drives, e.g. Vagrant
                        throw new UncheckedIOException(
                            "Failed to create hard link " + destination + " pointing to " + source, e
                        );
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Can't walk source " + sourceRoot, e);
        }
    }

    private void createConfiguration()  {
        defaultConfig.put("node.name", nameCustomization.apply(safeName(name)));
        defaultConfig.put("path.repo", confPathRepo.toAbsolutePath().toString());
        defaultConfig.put("path.data", confPathData.toAbsolutePath().toString());
        defaultConfig.put("path.logs", confPathLogs.toAbsolutePath().toString());
        defaultConfig.put("path.shared_data", workingDir.resolve("sharedData").toString());
        defaultConfig.put("node.attr.testattr", "test");
        defaultConfig.put("node.portsfile", "true");
        defaultConfig.put("http.port", "0");
        if (Version.fromString(version).onOrAfter(Version.fromString("6.7.0"))) {
            defaultConfig.put("transport.port", "0");
        } else {
            defaultConfig.put("transport.tcp.port", "0");
        }
        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        defaultConfig.put("cluster.routing.allocation.disk.watermark.low", "1b");
        defaultConfig.put("cluster.routing.allocation.disk.watermark.high", "1b");
        // increase script compilation limit since tests can rapid-fire script compilations
        defaultConfig.put("script.max_compilations_rate", "2048/1m");
        if (Version.fromString(version).getMajor() >= 6) {
            defaultConfig.put("cluster.routing.allocation.disk.watermark.flood_stage", "1b");
        }
        // Temporarily disable the real memory usage circuit breaker. It depends on real memory usage which we have no full control
        // over and the REST client will not retry on circuit breaking exceptions yet (see #31986 for details). Once the REST client
        // can retry on circuit breaking exceptions, we can revert again to the default configuration.
        if (Version.fromString(version).getMajor() >= 7) {
            defaultConfig.put("indices.breaker.total.use_real_memory",  "false");
        }
        // Don't wait for state, just start up quickly. This will also allow new and old nodes in the BWC case to become the master
        defaultConfig.put("discovery.initial_state_timeout",  "0s");

        checkSuppliers("Settings", settings.values());
        Map<String, String> userConfig = settings.entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get().toString()));
        HashSet<String> overriden = new HashSet<>(defaultConfig.keySet());
        overriden.retainAll(userConfig.keySet());
        overriden.removeAll(OVERRIDABLE_SETTINGS);
        if (overriden.isEmpty() ==false) {
            throw new IllegalArgumentException(
                "Testclusters does not allow the following settings to be changed:" + overriden + " for " + this
            );
        }
        // Make sure no duplicate config keys
        userConfig.keySet().stream()
            .filter(OVERRIDABLE_SETTINGS::contains)
            .forEach(defaultConfig::remove);

        try {
            // We create hard links  for the distribution, so we need to remove the config file before writing it
            // to prevent the changes to reflect across all copies.
            Files.delete(configFile);
            Files.write(
                configFile,
                Stream.concat(
                    userConfig.entrySet().stream(),
                    defaultConfig.entrySet().stream()
                )
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n"))
                    .getBytes(StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write config file: " + configFile, e);
        }
        LOGGER.info("Written config file:{} for {}", configFile, this);
    }

    private void checkFrozen() {
        if (configurationFrozen.get()) {
            throw new IllegalStateException("Configuration for " + this +  " can not be altered, already locked");
        }
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
        try(Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            return lines.map(String::trim).collect(Collectors.toList());
        }
    }

    @Override
    public boolean isProcessAlive() {
        requireNonNull(
            esProcess,
            "Can't wait for `" + this + "` as it's not started. Does the task have `useCluster` ?"
        );
        return esProcess.isAlive();
    }

    void waitForAllConditions() {
        waitForConditions(waitConditions, System.currentTimeMillis(), NODE_UP_TIMEOUT, NODE_UP_TIMEOUT_UNIT, this);
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

    List<Map<String, String>> getCredentials() {
        return credentials;
    }

    private boolean checkPortsFilesExistWithDelay(TestClusterConfiguration node) {
        if (Files.exists(httpPortsFile) && Files.exists(transportPortFile)) {
            return true;
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TestClustersException("Interrupted while waiting for ports files", e);
        }
        return Files.exists(httpPortsFile) && Files.exists(transportPortFile);
    }

    public boolean isHttpSslEnabled() {
        return Boolean.valueOf(
            settings.getOrDefault("xpack.security.http.ssl.enabled", () -> "false").get().toString()
        );
    }

    void configureHttpWait(WaitForHttpResource wait) {
        if (settings.containsKey("xpack.security.http.ssl.certificate_authorities")) {
            wait.setCertificateAuthorities(
                getConfigDir()
                    .resolve(settings.get("xpack.security.http.ssl.certificate_authorities").get().toString())
                    .toFile()
            );
        }
        if (settings.containsKey("xpack.security.http.ssl.certificate")) {
            wait.setCertificateAuthorities(
                getConfigDir()
                    .resolve(settings.get("xpack.security.http.ssl.certificate").get().toString())
                    .toFile()
            );
        }
        if (settings.containsKey("xpack.security.http.ssl.keystore.path")) {
            wait.setTrustStoreFile(
                getConfigDir()
                    .resolve(settings.get("xpack.security.http.ssl.keystore.path").get().toString())
                    .toFile()
            );
        }
        if (keystoreSettings.containsKey("xpack.security.http.ssl.keystore.secure_password")) {
            wait.setTrustStorePassword(
                keystoreSettings.get("xpack.security.http.ssl.keystore.secure_password").get().toString()
            );
        }
    }
}
