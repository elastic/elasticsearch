/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.ReaperPlugin;
import org.elasticsearch.gradle.ReaperService;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.UntrackedTask;
import org.gradle.api.tasks.options.Option;
import org.gradle.api.tasks.options.OptionValues;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * Runs an Elasticsearch cluster in the foreground of the build until the build is cancelled.
 * <p>
 * The cluster is started by the {@code :test:test-clusters-cli} runner in a separate JVM, so it is configured exactly like the
 * clusters used by integration tests. Node output is streamed to the build log. All ports ({@link #getHttpPort()},
 * {@link #getTransportPort()}, {@link #getRemoteClusterPort()} and the debug ports) are <em>starting</em> ports: node {@code i} of a
 * multi-node cluster uses {@code port + i}.
 * <p>
 * Plugins and modules installed into the cluster are everything resolved into {@link #getPluginsPath()} and
 * {@link #getModulesPath()} respectively. Use {@link #plugin(String)} and {@link #module(String)} to add project bundles.
 */
@UntrackedTask(because = "When we wanna run a cluster, we wanna run a cluster.")
public abstract class RunTask extends DefaultTask {

    static final String RUNNER_MAIN_CLASS = "org.elasticsearch.test.cluster.cli.ClusterRunner";
    // Must match the plugin zip pattern used by the test-clusters library, e.g. analysis-icu-9.1.0-SNAPSHOT.zip
    private static final Pattern PLUGIN_ZIP_PATTERN = Pattern.compile("(.+)(?:-\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?\\.zip)");

    private String pluginsConfigurationName;
    private String modulesConfigurationName;

    @Inject
    protected abstract WorkerExecutor getWorkerExecutor();

    // ---- Cluster configuration ----

    @Input
    public abstract Property<String> getClusterName();

    /**
     * The distribution type, {@code DEFAULT} or {@code INTEG_TEST} (case-insensitive).
     */
    @Input
    public abstract Property<String> getDistribution();

    /**
     * The self-generated license type, {@code basic} or {@code trial}. Requires the default distribution.
     */
    @Option(option = "license", description = "Self-generated license type, requires the default distribution")
    @Input
    @Optional
    public abstract Property<String> getLicense();

    @OptionValues("license")
    public List<String> getLicenseOptionValues() {
        return RunClusterArgs.LICENSE_TYPES;
    }

    @Option(option = "nodes", description = "Number of nodes to start in the cluster")
    @Input
    public abstract Property<Integer> getNumberOfNodes();

    /**
     * Starting HTTP port; node {@code i} uses {@code httpPort + i}.
     */
    @Input
    public abstract Property<Integer> getHttpPort();

    /**
     * Starting transport port; node {@code i} uses {@code transportPort + i}.
     */
    @Input
    public abstract Property<Integer> getTransportPort();

    /**
     * Starting remote cluster server port; node {@code i} uses {@code remoteClusterPort + i}. Not set by default.
     */
    @Input
    @Optional
    public abstract Property<Integer> getRemoteClusterPort();

    /**
     * Starting port the server JVMs connect to when {@code --debug-jvm} is used; node {@code i} uses {@code port + i}.
     */
    @Input
    public abstract Property<Integer> getDebugServerPort();

    /**
     * Starting port the CLI launcher JVMs connect to when {@code --debug-cli-jvm} is used; node {@code i} uses {@code port + i}.
     */
    @Input
    public abstract Property<Integer> getDebugCliPort();

    @Input
    public abstract MapProperty<String, String> getSettings();

    /**
     * Settings for individual nodes, as {@code <index>:<key>=<value>}. Use {@link #nodeSetting(int, String, Object)} to add them.
     */
    @Input
    public abstract ListProperty<String> getNodeSettings();

    /**
     * Settings passed on the command line as {@code -Dtests.es.<setting>=<value>}. These take precedence over {@link #getSettings()}.
     */
    @Input
    public abstract MapProperty<String, String> getCommandLineSettings();

    @Input
    public abstract MapProperty<String, String> getSystemProperties();

    @Input
    public abstract ListProperty<String> getJvmArgs();

    @Input
    public abstract MapProperty<String, String> getKeystoreSettings();

    @Internal
    public abstract MapProperty<String, File> getKeystoreFiles();

    @Internal
    public abstract MapProperty<String, File> getConfigFiles();

    /**
     * Paths of the projects whose plugin bundles are installed. The bundles themselves are tracked by {@link #getPluginsPath()}.
     */
    @Internal
    public abstract ListProperty<String> getPluginProjects();

    /**
     * Paths of the projects whose module bundles are installed. The bundles themselves are tracked by {@link #getModulesPath()}.
     */
    @Internal
    public abstract ListProperty<String> getModuleProjects();

    @Input
    public abstract ListProperty<RunUser> getUsers();

    /**
     * Heap size of each node, defaults to the {@code tests.heap.size} system property, or 512m.
     */
    @Input
    public abstract Property<String> getHeapSize();

    /**
     * Extra JVM arguments for both the server and CLI JVMs, defaults to the {@code tests.jvm.argline} system property.
     */
    @Input
    @Optional
    public abstract Property<String> getJvmArgline();

    /**
     * Base directory for node working directories, i.e. node {@code n} uses {@code <workingDir>/<n>}.
     */
    @Internal
    public abstract DirectoryProperty getWorkingDir();

    @Internal
    public abstract DirectoryProperty getTlsResourcesDir();

    // ---- Runtime wiring ----

    @Classpath
    public abstract ConfigurableFileCollection getRunnerClasspath();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getDistributionDir();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getPluginsPath();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getModulesPath();

    @Internal
    public abstract Property<File> getRuntimeJavaHome();

    @Input
    public abstract Property<Boolean> getSnapshotBuild();

    @ServiceReference(ReaperPlugin.REAPER_SERVICE_NAME)
    protected abstract Property<ReaperService> getReaper();

    // ---- Command line options ----

    @Option(option = "debug-jvm", description = "Connect each node to a listening debugger, starting at port 5007")
    @Input
    public abstract Property<Boolean> getDebugJvm();

    @Option(option = "debug-cli-jvm", description = "Connect each node's CLI launcher to a listening debugger, starting at port 5107")
    @Input
    public abstract Property<Boolean> getDebugCliJvm();

    @Option(option = "preserve-data", description = "Preserves data directory contents (path provided to --data-dir is always preserved)")
    @Input
    public abstract Property<Boolean> getPreserveData();

    @Option(option = "data-dir", description = "Override the base data directory used by the cluster")
    @Input
    @Optional
    public abstract Property<String> getDataDir();

    @Option(option = "keystore-password", description = "Set the elasticsearch keystore password")
    @Input
    @Optional
    public abstract Property<String> getKeystorePassword();

    @Option(option = "https", description = "Helper option to enable HTTPS")
    @Input
    public abstract Property<Boolean> getUseHttps();

    @Option(option = "transport-tls", description = "Helper option to enable TLS on transport port")
    @Input
    public abstract Property<Boolean> getUseTransportTls();

    @Option(option = "with-plugins", description = "Comma-separated list of plugins from :plugins to install")
    @Input
    @Optional
    public abstract Property<String> getWithPlugins();

    @Option(option = "with-apm-server", description = "Run a mock OTLP/gRPC server that logs the telemetry the node exports")
    @Input
    public abstract Property<Boolean> getApmServerEnabled();

    @Option(option = "apm-metrics", description = "Metric wildcard filter for APM server")
    @Input
    @Optional
    public abstract Property<String> getApmServerMetrics();

    /**
     * Settings passed as {@code --setting <key>=<value>}. These take precedence over all other settings.
     */
    @Option(option = "setting", description = "Elasticsearch setting as <key>=<value>, may be repeated")
    @Input
    public abstract ListProperty<String> getSettingOptions();

    @Option(option = "system-property", description = "JVM system property as <key>=<value>, may be repeated")
    @Input
    public abstract ListProperty<String> getSystemPropertyOptions();

    @Option(option = "jvm-arg", description = "Argument for the server and CLI launcher JVMs, may be repeated")
    @Input
    public abstract ListProperty<String> getJvmArgOptions();

    // ---- DSL ----

    public void setting(String key, Object value) {
        if (value instanceof Provider<?> provider) {
            getSettings().put(key, provider.map(String::valueOf));
        } else {
            getSettings().put(key, String.valueOf(value));
        }
    }

    /**
     * Adds a setting to the node at the given (0-based) index only. These take precedence over cluster-wide settings.
     */
    public void nodeSetting(int index, String key, Object value) {
        getNodeSettings().add(index + ":" + key + "=" + value);
    }

    public void systemProperty(String key, Object value) {
        if (value instanceof Provider<?> provider) {
            getSystemProperties().put(key, provider.map(String::valueOf));
        } else {
            getSystemProperties().put(key, String.valueOf(value));
        }
    }

    public void jvmArgs(String... args) {
        getJvmArgs().addAll(args);
    }

    /**
     * Adds a keystore entry. File values become file settings, anything else a string setting.
     */
    public void keystore(String key, Object value) {
        if (value instanceof File file) {
            getKeystoreFiles().put(key, file);
        } else {
            getKeystoreSettings().put(key, String.valueOf(value));
        }
    }

    public void extraConfigFile(String destination, File source) {
        getConfigFiles().put(destination, source);
    }

    /**
     * Adds a file realm user, e.g. {@code user username: 'admin', password: 'admin-password', role: 'superuser'}.
     */
    public void user(Map<String, String> userSpec) {
        String username = userSpec.get("username");
        String password = userSpec.get("password");
        if (username == null || password == null) {
            throw new IllegalArgumentException("user requires 'username' and 'password', got " + userSpec.keySet());
        }
        getUsers().add(new RunUser(username, password, userSpec.get("role")));
    }

    /**
     * Installs the plugin built by the given project.
     */
    public void plugin(String projectPath) {
        getPluginProjects().add(projectPath);
    }

    /**
     * Installs the module built by the given project. Only applies to the {@code INTEG_TEST} distribution.
     */
    public void module(String projectPath) {
        getModuleProjects().add(projectPath);
    }

    /**
     * Uses the command line options of the given lifecycle task as defaults for this task, so options passed to the lifecycle
     * task apply to each of the clusters it runs.
     */
    public void inheritOptionsFrom(TaskProvider<RunLifecycleTask> lifecycleTask) {
        getDebugJvm().convention(lifecycleTask.flatMap(RunLifecycleTask::getDebugJvm));
        getDebugCliJvm().convention(lifecycleTask.flatMap(RunLifecycleTask::getDebugCliJvm));
        getPreserveData().convention(lifecycleTask.flatMap(RunLifecycleTask::getPreserveData));
    }

    void setConfigurationNames(String pluginsConfigurationName, String modulesConfigurationName) {
        this.pluginsConfigurationName = pluginsConfigurationName;
        this.modulesConfigurationName = modulesConfigurationName;
    }

    String pluginsConfigurationName() {
        return pluginsConfigurationName;
    }

    String modulesConfigurationName() {
        return modulesConfigurationName;
    }

    @TaskAction
    public void run() {
        String distribution = getDistribution().get().toUpperCase(Locale.ROOT);
        if (getApmServerEnabled().get() && distribution.equals("DEFAULT") == false) {
            throw new GradleException("--with-apm-server requires the default distribution, as the APM module provides telemetry");
        }
        String clusterName = getClusterName().get();
        File workingDir = getWorkingDir().get().getAsFile();
        File pidFile = new File(workingDir, clusterName + ".pids");

        Map<String, String> settings = new LinkedHashMap<>(getSettings().get());
        settings.putAll(getCommandLineSettings().get());
        Map<String, String> systemProperties = new LinkedHashMap<>(getSystemProperties().get());
        List<String> jvmArgs = new ArrayList<>(getJvmArgs().get());
        jvmArgs.addAll(getJvmArgOptions().get());
        try {
            settings.putAll(RunClusterArgs.parseKeyValues("setting", getSettingOptions().get()));
            systemProperties.putAll(RunClusterArgs.parseKeyValues("system-property", getSystemPropertyOptions().get()));
        } catch (IllegalArgumentException e) {
            throw new GradleException(e.getMessage(), e);
        }

        RunClusterArgs args = new RunClusterArgs().clusterName(clusterName)
            .distribution(distribution)
            .license(getLicense().getOrNull())
            .numberOfNodes(getNumberOfNodes().get())
            .workingDir(workingDir)
            .preserveData(getPreserveData().get())
            .dataDir(getDataDir().isPresent() ? new File(getDataDir().get()).getAbsoluteFile() : null)
            .ports(getHttpPort().get(), getTransportPort().get(), getRemoteClusterPort().getOrNull())
            .settings(settings)
            .nodeSettings(getNodeSettings().get())
            .systemProperties(systemProperties)
            .jvmArgs(jvmArgs)
            .keystore(getKeystoreSettings().get(), getKeystoreFiles().get(), getKeystorePassword().getOrNull())
            .users(getUsers().get())
            .plugins(pluginNames(getPluginsPath()))
            .modules(moduleNames(getModulesPath()))
            .configFiles(getConfigFiles().get())
            .tls(getUseHttps().get(), getUseTransportTls().get(), getTlsResourcesDir().get().getAsFile())
            .telemetryEnabled(getApmServerEnabled().get())
            .debugServer(getDebugJvm().get(), getDebugServerPort().get())
            .debugCli(getDebugCliJvm().get(), getDebugCliPort().get())
            .pidFile(pidFile);

        List<String> arguments;
        try {
            arguments = args.build();
        } catch (IllegalArgumentException e) {
            throw new GradleException(e.getMessage(), e);
        }

        Map<String, String> runnerSystemProperties = new LinkedHashMap<>();
        String distributionSysprop = distribution.equals("INTEG_TEST") ? "tests.integ-test.distribution" : "tests.default.distribution";
        runnerSystemProperties.put(distributionSysprop, getDistributionDir().getSingleFile().getAbsolutePath());
        runnerSystemProperties.put("tests.cluster.plugins.path", getPluginsPath().getAsPath());
        runnerSystemProperties.put("tests.cluster.modules.path", getModulesPath().getAsPath());
        runnerSystemProperties.put("tests.heap.size", getHeapSize().get());
        if (getJvmArgline().isPresent()) {
            runnerSystemProperties.put("tests.jvm.argline", getJvmArgline().get());
        }
        runnerSystemProperties.put("tests.task", getPath());
        runnerSystemProperties.put("build.snapshot", String.valueOf(getSnapshotBuild().get()));

        String java = new File(
            getRuntimeJavaHome().get(),
            OS.<String>conditional().onWindows(() -> "bin/java.exe").onUnix(() -> "bin/java").supply()
        ).getAbsolutePath();

        getWorkerExecutor().noIsolation().submit(RunClusterWorkAction.class, params -> {
            params.getTaskPath().set(getPath());
            params.getClusterName().set(clusterName);
            params.getExecutable().set(java);
            params.getClasspath().from(getRunnerClasspath());
            params.getMainClass().set(RUNNER_MAIN_CLASS);
            params.getArguments().set(arguments);
            params.getSystemProperties().set(runnerSystemProperties);
            params.getPidFile().set(pidFile);
            params.getApmServerEnabled().set(getApmServerEnabled());
            params.getApmServerMetrics().set(getApmServerMetrics());
            params.getReaper().set(getReaper());
        });
    }

    /**
     * Plugin names as matched by the test-clusters library, derived from the plugin zip file names.
     */
    static List<String> pluginNames(FileCollection pluginsPath) {
        List<String> names = new ArrayList<>();
        for (File file : pluginsPath.getFiles()) {
            Matcher matcher = PLUGIN_ZIP_PATTERN.matcher(file.getName());
            if (matcher.matches() == false) {
                throw new GradleException("Unexpected plugin bundle [" + file + "], expected <name>-<version>.zip");
            }
            names.add(matcher.group(1));
        }
        return names;
    }

    /**
     * Module names as matched by the test-clusters library, i.e. the exploded bundle directory names.
     */
    static List<String> moduleNames(FileCollection modulesPath) {
        return modulesPath.getFiles().stream().map(File::getName).toList();
    }

    /**
     * Splits the comma-separated {@code --with-plugins} option value.
     */
    static List<String> splitPluginNames(String value) {
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toList();
    }
}
