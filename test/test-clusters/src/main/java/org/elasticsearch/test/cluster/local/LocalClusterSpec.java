/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ClusterSpec;
import org.elasticsearch.test.cluster.EnvironmentProvider;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.SystemPropertyProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalClusterSpec implements ClusterSpec {
    private final String name;
    private final List<User> users;
    private final List<Resource> roleFiles;
    private final boolean shared;
    private List<LocalNodeSpec> nodes;

    public LocalClusterSpec(String name, List<User> users, List<Resource> roleFiles, boolean shared) {
        this.name = name;
        this.users = users;
        this.roleFiles = roleFiles;
        this.shared = shared;
    }

    public String getName() {
        return name;
    }

    public List<User> getUsers() {
        return users;
    }

    public List<Resource> getRoleFiles() {
        return roleFiles;
    }

    public List<LocalNodeSpec> getNodes() {
        return nodes;
    }

    public boolean isShared() {
        return shared;
    }

    public void setNodes(List<LocalNodeSpec> nodes) {
        this.nodes = nodes;
    }

    void validate() {
        // Ensure we don't have nodes with duplicate names
        List<String> nodeNames = nodes.stream().map(LocalNodeSpec::getName).collect(Collectors.toList());
        Set<String> uniqueNames = nodes.stream().map(LocalNodeSpec::getName).collect(Collectors.toSet());
        uniqueNames.forEach(name -> nodeNames.remove(nodeNames.indexOf(name)));

        if (nodeNames.isEmpty() == false) {
            throw new IllegalArgumentException("Cluster cannot contain nodes with duplicates names: " + nodeNames);
        }

        // Ensure we do not configure older version nodes with the integTest distribution
        if (nodes.stream().anyMatch(n -> n.getVersion() != Version.CURRENT && n.getDistributionType() == DistributionType.INTEG_TEST)) {
            throw new IllegalArgumentException(
                "Error configuring test cluster '"
                    + name
                    + "'. When configuring a node for a prior Elasticsearch version, the default distribution type must be used."
            );
        }
    }

    public static class LocalNodeSpec {
        private final LocalClusterSpec cluster;
        private final String name;
        private final List<SettingsProvider> settingsProviders;
        private final Map<String, String> settings;
        private final List<EnvironmentProvider> environmentProviders;
        private final Map<String, String> environment;
        private final Map<String, DefaultPluginInstallSpec> modules;
        private final Map<String, DefaultPluginInstallSpec> plugins;
        private final DistributionType distributionType;
        private final Set<FeatureFlag> features;
        private final List<SettingsProvider> keystoreProviders;
        private final Map<String, String> keystoreSettings;
        private final Map<String, Resource> keystoreFiles;
        private final String keystorePassword;
        private final Map<String, Resource> extraConfigFiles;
        private final List<SystemPropertyProvider> systemPropertyProviders;
        private final Map<String, String> systemProperties;
        private final List<String> jvmArgs;
        private final Path configDir;
        private Version version;

        public LocalNodeSpec(
            LocalClusterSpec cluster,
            String name,
            Version version,
            List<SettingsProvider> settingsProviders,
            Map<String, String> settings,
            List<EnvironmentProvider> environmentProviders,
            Map<String, String> environment,
            Map<String, DefaultPluginInstallSpec> modules,
            Map<String, DefaultPluginInstallSpec> plugins,
            DistributionType distributionType,
            Set<FeatureFlag> features,
            List<SettingsProvider> keystoreProviders,
            Map<String, String> keystoreSettings,
            Map<String, Resource> keystoreFiles,
            String keystorePassword,
            Map<String, Resource> extraConfigFiles,
            List<SystemPropertyProvider> systemPropertyProviders,
            Map<String, String> systemProperties,
            List<String> jvmArgs,
            Path configDir
        ) {
            this.cluster = cluster;
            this.name = name;
            this.version = version;
            this.settingsProviders = settingsProviders;
            this.settings = settings;
            this.environmentProviders = environmentProviders;
            this.environment = environment;
            this.modules = modules;
            this.plugins = plugins;
            this.distributionType = distributionType;
            this.features = features;
            this.keystoreProviders = keystoreProviders;
            this.keystoreSettings = keystoreSettings;
            this.keystoreFiles = keystoreFiles;
            this.keystorePassword = keystorePassword;
            this.extraConfigFiles = extraConfigFiles;
            this.systemPropertyProviders = systemPropertyProviders;
            this.systemProperties = systemProperties;
            this.jvmArgs = jvmArgs;
            this.configDir = configDir;
        }

        void setVersion(Version version) {
            this.version = version;
        }

        public LocalClusterSpec getCluster() {
            return cluster;
        }

        public String getName() {
            return name;
        }

        public Version getVersion() {
            return version;
        }

        public List<User> getUsers() {
            return cluster.getUsers();
        }

        public List<Resource> getRolesFiles() {
            return cluster.getRoleFiles();
        }

        public DistributionType getDistributionType() {
            return distributionType;
        }

        public Map<String, DefaultPluginInstallSpec> getModules() {
            return modules;
        }

        public Map<String, DefaultPluginInstallSpec> getPlugins() {
            return plugins;
        }

        public Set<FeatureFlag> getFeatures() {
            return features;
        }

        public Map<String, Resource> getKeystoreFiles() {
            return keystoreFiles;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public Map<String, Resource> getExtraConfigFiles() {
            return extraConfigFiles;
        }

        public List<String> getJvmArgs() {
            return jvmArgs;
        }

        public Path getConfigDir() {
            return configDir;
        }

        public boolean isSecurityEnabled() {
            return Boolean.parseBoolean(
                getSetting(
                    "xpack.security.enabled",
                    getVersion().equals(Version.fromString("0.0.0")) || getVersion().onOrAfter("8.0.0") ? "true" : "false"
                )
            );
        }

        public boolean isRemoteClusterServerEnabled() {
            return Boolean.parseBoolean(getSetting("remote_cluster_server.enabled", "false"));
        }

        public boolean isMasterEligible() {
            return getSetting("node.roles", "master").contains("master");
        }

        public boolean hasRole(String role) {
            return getSetting("node.roles", "[]").contains(role);
        }

        /**
         * Return node configured setting or the provided default if no explicit value has been configured. This method returns all
         * settings, to include security settings provided to the keystore
         *
         * @param setting the setting name
         * @param defaultValue a default value
         * @return the configured setting value or provided default
         */
        public String getSetting(String setting, String defaultValue) {
            Map<String, String> allSettings = new HashMap<>();
            allSettings.putAll(resolveSettings());
            allSettings.putAll(resolveKeystore());

            return allSettings.getOrDefault(setting, defaultValue);
        }

        /**
         * Resolve node settings. Order of precedence is as follows:
         * <ol>
         *     <li>Settings from cluster configured {@link SettingsProvider}</li>
         *     <li>Settings from node configured {@link SettingsProvider}</li>
         *     <li>Explicit cluster settings</li>
         *     <li>Explicit node settings</li>
         * </ol>
         *
         * @return resolved settings for node
         */
        public Map<String, String> resolveSettings() {
            Map<String, String> resolvedSettings = new HashMap<>();
            settingsProviders.forEach(p -> resolvedSettings.putAll(p.get(getFilteredSpec(p, null))));
            resolvedSettings.putAll(settings);
            return resolvedSettings;
        }

        /**
         * Resolve secure keystore settings. Order of precedence is as follows:
         * <ol>
         *     <li>Keystore from cluster configured {@link SettingsProvider}</li>
         *     <li>Keystore from node configured {@link SettingsProvider}</li>
         *     <li>Explicit cluster secure settings</li>
         *     <li>Explicit node secure settings</li>
         * </ol>
         *
         * @return resolved settings for node
         */
        public Map<String, String> resolveKeystore() {
            Map<String, String> resolvedKeystore = new HashMap<>();
            keystoreProviders.forEach(p -> resolvedKeystore.putAll(p.get(getFilteredSpec(null, p))));
            resolvedKeystore.putAll(keystoreSettings);
            return resolvedKeystore;
        }

        /**
         * Resolve node environment variables. Order of precedence is as follows:
         * <ol>
         *     <li>Environment variables from cluster configured {@link EnvironmentProvider}</li>
         *     <li>Environment variables from node configured {@link EnvironmentProvider}</li>
         *     <li>Environment variables cluster settings</li>
         *     <li>Environment variables node settings</li>
         * </ol>
         *
         * @return resolved environment variables for node
         */
        public Map<String, String> resolveEnvironment() {
            Map<String, String> resolvedEnvironment = new HashMap<>();
            environmentProviders.forEach(p -> resolvedEnvironment.putAll(p.get(this)));
            resolvedEnvironment.putAll(environment);
            return resolvedEnvironment;
        }

        /**
         * Resolve node system properties. Order of precedence is as follows:
         * <ol>
         *     <li>SystemProperties from cluster configured {@link SystemPropertyProvider}</li>
         *     <li>SystemProperties variables from node configured {@link SystemPropertyProvider}</li>
         *     <li>SystemProperties variables cluster settings</li>
         *     <li>SystemProperties variables node settings</li>
         * </ol>
         *
         * @return resolved system properties for node
         */
        public Map<String, String> resolveSystemProperties() {
            Map<String, String> resolvedSystemProperties = new HashMap<>();
            systemPropertyProviders.forEach(p -> resolvedSystemProperties.putAll(p.get(this)));
            resolvedSystemProperties.putAll(systemProperties);
            return resolvedSystemProperties;
        }

        /**
         * Returns a new {@link LocalNodeSpec} without the given {@link SettingsProvider}s. This is needed when resolving settings from a
         * settings provider to avoid infinite recursion.
         *
         * @param filteredProvider the provider to omit from the new node spec
         * @param filteredKeystoreProvider the keystore provider to omit from the new node spec
         * @return a new local node spec
         */
        private LocalNodeSpec getFilteredSpec(SettingsProvider filteredProvider, SettingsProvider filteredKeystoreProvider) {
            LocalClusterSpec newCluster = new LocalClusterSpec(cluster.name, cluster.users, cluster.roleFiles, cluster.shared);

            List<LocalNodeSpec> nodeSpecs = cluster.nodes.stream()
                .map(
                    n -> new LocalNodeSpec(
                        newCluster,
                        n.name,
                        n.version,
                        n.settingsProviders.stream().filter(s -> s != filteredProvider).toList(),
                        n.settings,
                        n.environmentProviders,
                        n.environment,
                        n.modules,
                        n.plugins,
                        n.distributionType,
                        n.features,
                        n.keystoreProviders.stream().filter(s -> s != filteredKeystoreProvider).toList(),
                        n.keystoreSettings,
                        n.keystoreFiles,
                        n.keystorePassword,
                        n.extraConfigFiles,
                        n.systemPropertyProviders,
                        n.systemProperties,
                        n.jvmArgs,
                        n.configDir
                    )
                )
                .toList();

            newCluster.setNodes(nodeSpecs);

            return nodeSpecs.stream().filter(n -> Objects.equals(n.getName(), this.getName())).findFirst().get();
        }
    }
}
