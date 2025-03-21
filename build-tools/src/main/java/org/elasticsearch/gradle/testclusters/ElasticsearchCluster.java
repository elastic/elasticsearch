/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.FileSupplier;
import org.elasticsearch.gradle.PropertyNormalization;
import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.Version;
import org.gradle.api.Named;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.RegularFile;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.plugin.BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG;
import static org.elasticsearch.gradle.testclusters.TestClustersPlugin.BUNDLE_ATTRIBUTE;

public class ElasticsearchCluster implements TestClusterConfiguration, Named {

    private static final Logger LOGGER = Logging.getLogger(ElasticsearchNode.class);
    private static final int CLUSTER_UP_TIMEOUT = 40;
    private static final TimeUnit CLUSTER_UP_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private final AtomicBoolean configurationFrozen = new AtomicBoolean(false);
    private final String path;
    private final String clusterName;
    private final NamedDomainObjectContainer<ElasticsearchNode> nodes;
    private final FileOperations fileOperations;
    private final File workingDirBase;
    private final LinkedHashMap<String, Predicate<TestClusterConfiguration>> waitConditions = new LinkedHashMap<>();
    private final transient Project project;
    private final Provider<ReaperService> reaper;
    private final Provider<TestClustersRegistry> testClustersRegistryProvider;
    private final FileSystemOperations fileSystemOperations;
    private final ArchiveOperations archiveOperations;
    private final ExecOperations execOperations;
    private final Provider<File> runtimeJava;
    private final Function<Version, Boolean> isReleasedVersion;
    private int nodeIndex = 0;

    private final ConfigurableFileCollection pluginAndModuleConfiguration;

    private boolean shared = false;

    private int claims = 0;

    public ElasticsearchCluster(
        String path,
        String clusterName,
        Project project,
        Provider<ReaperService> reaper,
        Provider<TestClustersRegistry> testClustersRegistryProvider,
        FileSystemOperations fileSystemOperations,
        ArchiveOperations archiveOperations,
        ExecOperations execOperations,
        FileOperations fileOperations,
        File workingDirBase,
        Provider<File> runtimeJava,
        Function<Version, Boolean> isReleasedVersion
    ) {
        this.path = path;
        this.clusterName = clusterName;
        this.project = project;
        this.reaper = reaper;
        this.testClustersRegistryProvider = testClustersRegistryProvider;
        this.fileSystemOperations = fileSystemOperations;
        this.archiveOperations = archiveOperations;
        this.execOperations = execOperations;
        this.fileOperations = fileOperations;
        this.workingDirBase = workingDirBase;
        this.runtimeJava = runtimeJava;
        this.isReleasedVersion = isReleasedVersion;
        this.nodes = project.container(ElasticsearchNode.class);
        this.pluginAndModuleConfiguration = project.getObjects().fileCollection();
        this.nodes.add(
            new ElasticsearchNode(
                safeName(clusterName),
                path,
                clusterName + "-0",
                project,
                reaper,
                testClustersRegistryProvider,
                fileSystemOperations,
                archiveOperations,
                execOperations,
                fileOperations,
                workingDirBase,
                runtimeJava,
                isReleasedVersion
            )
        );

        addWaitForClusterHealth();
    }

    /**
     * this cluster si marked as shared across TestClusterAware tasks
     * */
    @Internal
    public boolean isShared() {
        return shared;
    }

    protected void setShared(boolean shared) {
        this.shared = shared;
    }

    @Classpath
    public FileCollection getInstalledClasspath() {
        return pluginAndModuleConfiguration.getAsFileTree().filter(f -> f.getName().endsWith(".jar"));
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getInstalledFiles() {
        return pluginAndModuleConfiguration.getAsFileTree().filter(f -> f.getName().endsWith(".jar") == false);
    }

    public void setNumberOfNodes(int numberOfNodes) {
        checkFrozen();

        if (numberOfNodes < 1) {
            throw new IllegalArgumentException("Number of nodes should be >= 1 but was " + numberOfNodes + " for " + this);
        }

        if (numberOfNodes < nodes.size()) {
            throw new IllegalArgumentException(
                "Cannot shrink " + this + " to have " + numberOfNodes + " nodes as it already has " + getNumberOfNodes()
            );
        }

        for (int i = nodes.size(); i < numberOfNodes; i++) {
            this.nodes.add(
                new ElasticsearchNode(
                    safeName(clusterName),
                    path,
                    clusterName + "-" + i,
                    project,
                    reaper,
                    testClustersRegistryProvider,
                    fileSystemOperations,
                    archiveOperations,
                    execOperations,
                    fileOperations,
                    workingDirBase,
                    runtimeJava,
                    isReleasedVersion
                )
            );
        }
    }

    public void setReadinessEnabled(boolean enabled) {
        if (enabled) {
            for (ElasticsearchNode node : nodes) {
                node.setting("readiness.port", "0"); // ephemeral port
            }
        }
    }

    @Internal
    public ElasticsearchNode getFirstNode() {
        return nodes.getAt(clusterName + "-0");
    }

    @Internal
    public ElasticsearchNode getLastNode() {
        int index = nodes.size() - 1;
        return nodes.getAt(clusterName + "-" + index);
    }

    @Internal
    public int getNumberOfNodes() {
        return nodes.size();
    }

    @Internal
    public String getName() {
        return clusterName;
    }

    @Internal
    public String getPath() {
        return path;
    }

    @Override
    public void setVersion(String version) {
        nodes.all(each -> each.setVersion(version));
    }

    @Override
    public void setVersions(List<String> version) {
        nodes.all(each -> each.setVersions(version));
    }

    @Override
    public void setTestDistribution(TestDistribution distribution) {
        nodes.all(each -> each.setTestDistribution(distribution));
    }

    private void registerExtractedConfig(Provider<RegularFile> pluginProvider) {
        Dependency pluginDependency = this.project.getDependencies().create(project.files(pluginProvider));
        Configuration extractedConfig = project.getConfigurations().detachedConfiguration(pluginDependency);
        extractedConfig.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        extractedConfig.getAttributes().attribute(BUNDLE_ATTRIBUTE, true);
        pluginAndModuleConfiguration.from(extractedConfig);
    }

    @Override
    public void plugin(String pluginProjectPath) {
        plugin(maybeCreatePluginOrModuleDependency(pluginProjectPath, "zip"));
    }

    public void plugin(TaskProvider<Zip> plugin) {
        plugin(plugin.flatMap(AbstractArchiveTask::getArchiveFile));
    }

    @Override
    public void plugin(Provider<RegularFile> plugin) {
        registerExtractedConfig(plugin);
        nodes.all(each -> each.plugin(plugin));
    }

    @Override
    public void module(Provider<RegularFile> module) {
        registerExtractedConfig(module);
        nodes.all(each -> each.module(module));
    }

    public void module(TaskProvider<Sync> module) {
        module(project.getLayout().file(module.map(Sync::getDestinationDir)));
    }

    @Override
    public void module(String moduleProjectPath) {
        module(maybeCreatePluginOrModuleDependency(moduleProjectPath, EXPLODED_BUNDLE_CONFIG));
    }

    private final Map<String, Configuration> pluginAndModuleConfigurations = new HashMap<>();

    // package protected so only TestClustersAware can access
    @Internal
    Collection<Configuration> getPluginAndModuleConfigurations() {
        return pluginAndModuleConfigurations.values();
    }

    // creates a configuration to depend on the given plugin project, then wraps that configuration
    // to grab the zip as a file provider
    private Provider<RegularFile> maybeCreatePluginOrModuleDependency(String path, String consumingConfiguration) {
        var configuration = pluginAndModuleConfigurations.computeIfAbsent(path, key -> {
            var bundleDependency = this.project.getDependencies().project(Map.of("path", path, "configuration", consumingConfiguration));
            return project.getConfigurations().detachedConfiguration(bundleDependency);
        });

        Provider<File> fileProvider = configuration.getElements()
            .map(
                s -> s.stream()
                    .findFirst()
                    .orElseThrow(
                        () -> new IllegalStateException(consumingConfiguration + " configuration of project " + path + " had no files")
                    )
                    .getAsFile()
            );
        return project.getLayout().file(fileProvider);
    }

    @Override
    public void keystore(String key, String value) {
        nodes.all(each -> each.keystore(key, value));
    }

    @Override
    public void keystore(String key, Supplier<CharSequence> valueSupplier) {
        nodes.all(each -> each.keystore(key, valueSupplier));
    }

    @Override
    public void keystore(String key, File value) {
        nodes.all(each -> each.keystore(key, value));
    }

    @Override
    public void keystore(String key, File value, PropertyNormalization normalization) {
        nodes.all(each -> each.keystore(key, value, normalization));
    }

    @Override
    public void keystore(String key, FileSupplier valueSupplier) {
        nodes.all(each -> each.keystore(key, valueSupplier));
    }

    @Override
    public void keystorePassword(String password) {
        nodes.all(each -> each.keystorePassword(password));
    }

    @Override
    public void cliSetup(String binTool, CharSequence... args) {
        nodes.all(each -> each.cliSetup(binTool, args));
    }

    @Override
    public void setting(String key, String value) {
        nodes.all(each -> each.setting(key, value));
    }

    @Override
    public void setting(String key, String value, PropertyNormalization normalization) {
        nodes.all(each -> each.setting(key, value, normalization));
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier) {
        nodes.all(each -> each.setting(key, valueSupplier));
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        nodes.all(each -> each.setting(key, valueSupplier, normalization));
    }

    @Override
    public void systemProperty(String key, String value) {
        nodes.all(each -> each.systemProperty(key, value));
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier) {
        nodes.all(each -> each.systemProperty(key, valueSupplier));
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        nodes.all(each -> each.systemProperty(key, valueSupplier, normalization));
    }

    @Override
    public void environment(String key, String value) {
        nodes.all(each -> each.environment(key, value));
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier) {
        nodes.all(each -> each.environment(key, valueSupplier));
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        nodes.all(each -> each.environment(key, valueSupplier, normalization));
    }

    @Override
    public void jvmArgs(String... values) {
        nodes.all(each -> each.jvmArgs(values));
    }

    @Internal
    public boolean isPreserveDataDir() {
        return nodes.stream().anyMatch(node -> node.isPreserveDataDir());
    }

    @Override
    public void setPreserveDataDir(boolean preserveDataDir) {
        nodes.all(each -> each.setPreserveDataDir(preserveDataDir));
    }

    @Override
    public void freeze() {
        nodes.forEach(ElasticsearchNode::freeze);
        configurationFrozen.set(true);
        nodes.whenObjectAdded(node -> { throw new IllegalStateException("Cannot add nodes to test cluster after is has been frozen"); });
    }

    private void checkFrozen() {
        if (configurationFrozen.get()) {
            throw new IllegalStateException("Configuration for " + this + " can not be altered, already locked");
        }
    }

    @Override
    public void start() {
        commonNodeConfig();
        nodes.forEach(ElasticsearchNode::start);
    }

    private void commonNodeConfig() {
        final String nodeNames;
        if (nodes.stream().map(ElasticsearchNode::getName).anyMatch(name -> name == null)) {
            nodeNames = null;
        } else {
            nodeNames = nodes.stream().map(ElasticsearchNode::getName).map(this::safeName).collect(Collectors.joining(","));
        }
        ElasticsearchNode firstNode = null;
        for (ElasticsearchNode node : nodes) {
            if (node.getTestDistribution().equals(TestDistribution.INTEG_TEST)) {
                node.defaultConfig.put("xpack.security.enabled", "false");
            } else {
                if (hasDeprecationIndexing(node)) {
                    node.defaultConfig.put("cluster.deprecation_indexing.enabled", "false");
                }
            }

            // Can only configure master nodes if we have node names defined
            if (nodeNames != null) {
                assert node.getVersion().onOrAfter("7.0.0") : node.getVersion();
                assert node.defaultConfig.keySet().stream().noneMatch(name -> name.startsWith("discovery.zen."));
                node.defaultConfig.put("cluster.initial_master_nodes", "[" + nodeNames + "]");
                node.defaultConfig.put("discovery.seed_providers", "file");
                node.defaultConfig.put("discovery.seed_hosts", "[]");
            }
            if (firstNode == null) {
                firstNode = node;
            }
        }
    }

    @Override
    public void restart() {
        nodes.forEach(ElasticsearchNode::restart);
    }

    public void goToNextVersion() {
        stop(false);
        nodes.all(ElasticsearchNode::goToNextVersion);
        start();
        writeUnicastHostsFiles();
    }

    public void nextNodeToNextVersion() {
        if (nodeIndex + 1 > nodes.size()) {
            throw new TestClustersException("Ran out of nodes to take to the next version");
        }
        ElasticsearchNode node = nodes.getByName(clusterName + "-" + nodeIndex);
        node.stop(false);
        node.goToNextVersion();
        commonNodeConfig();
        nodeIndex += 1;
        if (node.getTestDistribution().equals(TestDistribution.DEFAULT)) {
            if (hasDeprecationIndexing(node)) {
                node.setting("cluster.deprecation_indexing.enabled", "false");
            }
        }
        node.start();
    }

    private static boolean hasDeprecationIndexing(ElasticsearchNode node) {
        return node.getVersion().onOrAfter("7.16.0") && node.getSettingKeys().contains("stateless.enabled") == false;
    }

    @Override
    public void extraConfigFile(String destination, File from) {
        nodes.all(node -> node.extraConfigFile(destination, from));
    }

    @Override
    public void extraConfigFile(String destination, File from, PropertyNormalization normalization) {
        nodes.all(node -> node.extraConfigFile(destination, from, normalization));
    }

    @Override
    public void extraJarFiles(FileCollection from) {
        nodes.all(node -> node.extraJarFiles(from));
    }

    @Override
    public void user(Map<String, String> userSpec) {
        nodes.all(node -> node.user(userSpec));
    }

    @Override
    public void rolesFile(File rolesYml) {
        nodes.all(node -> node.rolesFile(rolesYml));
    }

    @Override
    public void requiresFeature(String feature, Version from) {
        nodes.all(node -> node.requiresFeature(feature, from));
    }

    @Override
    public void requiresFeature(String feature, Version from, Version until) {
        nodes.all(node -> node.requiresFeature(feature, from, until));
    }

    public void writeUnicastHostsFiles() {
        String unicastUris = nodes.stream().flatMap(node -> node.getAllTransportPortURI().stream()).collect(Collectors.joining("\n"));
        nodes.forEach(node -> {
            try {
                Files.writeString(node.getConfigDir().resolve("unicast_hosts.txt"), unicastUris);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write unicast_hosts for " + this, e);
            }
        });
    }

    @Override
    @Internal
    public String getHttpSocketURI() {
        waitForAllConditions();
        return getFirstNode().getHttpSocketURI();
    }

    @Override
    @Internal
    public String getTransportPortURI() {
        waitForAllConditions();
        return getFirstNode().getTransportPortURI();
    }

    @Override
    @Internal
    public String getReadinessPortURI() {
        waitForAllConditions();
        return getFirstNode().getReadinessPortURI();
    }

    @Override
    @Internal
    public List<String> getAllHttpSocketURI() {
        waitForAllConditions();
        return nodes.stream().flatMap(each -> each.getAllHttpSocketURI().stream()).collect(Collectors.toList());
    }

    @Override
    @Internal
    public List<String> getAllTransportPortURI() {
        waitForAllConditions();
        return nodes.stream().flatMap(each -> each.getAllTransportPortURI().stream()).collect(Collectors.toList());
    }

    @Override
    @Internal
    public List<String> getAllReadinessPortURI() {
        waitForAllConditions();
        return nodes.stream().flatMap(each -> each.getAllReadinessPortURI().stream()).collect(Collectors.toList());
    }

    @Override
    @Internal
    public List<String> getAllRemoteAccessPortURI() {
        waitForAllConditions();
        return nodes.stream().flatMap(each -> each.getAllRemoteAccessPortURI().stream()).collect(Collectors.toList());
    }

    public void waitForAllConditions() {
        writeUnicastHostsFiles();

        LOGGER.info("Starting to wait for cluster to form");
        waitForConditions(waitConditions, System.currentTimeMillis(), CLUSTER_UP_TIMEOUT, CLUSTER_UP_TIMEOUT_UNIT, this);
    }

    @Override
    public void stop(boolean tailLogs) {
        nodes.forEach(each -> each.stop(tailLogs));
    }

    @Override
    public void setNameCustomization(Function<String, String> nameCustomization) {
        nodes.all(each -> each.setNameCustomization(nameCustomization));
    }

    @Override
    @Internal
    public boolean isProcessAlive() {
        return nodes.stream().noneMatch(node -> node.isProcessAlive() == false);
    }

    public ElasticsearchNode singleNode() {
        if (nodes.size() != 1) {
            throw new IllegalStateException("Can't treat " + this + " as single node as it has " + nodes.size() + " nodes");
        }
        return getFirstNode();
    }

    private void addWaitForClusterHealth() {
        waitConditions.put("cluster health yellow", (node) -> {
            try {
                boolean httpSslEnabled = getFirstNode().isHttpSslEnabled();
                WaitForHttpResource wait = new WaitForHttpResource(
                    httpSslEnabled ? "https" : "http",
                    getFirstNode().getHttpSocketURI(),
                    nodes.size()
                );
                if (httpSslEnabled) {
                    getFirstNode().configureHttpWait(wait);
                }
                List<Map<String, String>> credentials = getFirstNode().getCredentials();
                if (getFirstNode().getCredentials().isEmpty() == false) {
                    wait.setUsername(credentials.get(0).get("useradd"));
                    wait.setPassword(credentials.get(0).get("-p"));
                }
                return wait.wait(500);
            } catch (IOException e) {
                throw new UncheckedIOException("IO error while waiting cluster", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TestClustersException("Interrupted while waiting for " + this, e);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException("security exception", e);
            }
        });
    }

    @Nested
    public NamedDomainObjectContainer<ElasticsearchNode> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchCluster that = (ElasticsearchCluster) o;
        return Objects.equals(clusterName, that.clusterName) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, path);
    }

    @Override
    public String toString() {
        return "cluster{" + path + ":" + clusterName + "}";
    }

    int addClaim() {
        return ++this.claims;
    }

    int removeClaim() {
        return --this.claims;
    }
}
