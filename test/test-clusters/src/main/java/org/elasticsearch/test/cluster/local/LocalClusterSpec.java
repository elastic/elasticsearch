/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ClusterSpec;
import org.elasticsearch.test.cluster.EnvironmentProvider;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.TextResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalClusterSpec implements ClusterSpec {
    private final String name;
    private final List<User> users;
    private final List<TextResource> roleFiles;
    private List<LocalNodeSpec> nodes;

    public LocalClusterSpec(String name, List<User> users, List<TextResource> roleFiles) {
        this.name = name;
        this.users = users;
        this.roleFiles = roleFiles;
    }

    public String getName() {
        return name;
    }

    public List<User> getUsers() {
        return users;
    }

    public List<TextResource> getRoleFiles() {
        return roleFiles;
    }

    public List<LocalNodeSpec> getNodes() {
        return nodes;
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
    }

    public static class LocalNodeSpec {
        private final LocalClusterSpec cluster;
        private final String name;
        private final Version version;
        private final List<SettingsProvider> settingsProviders;
        private final Map<String, String> settings;
        private final List<EnvironmentProvider> environmentProviders;
        private final Map<String, String> environment;
        private final Set<String> modules;
        private final Set<String> plugins;
        private final DistributionType distributionType;
        private final Set<FeatureFlag> features;

        public LocalNodeSpec(
            LocalClusterSpec cluster,
            String name,
            Version version,
            List<SettingsProvider> settingsProviders,
            Map<String, String> settings,
            List<EnvironmentProvider> environmentProviders,
            Map<String, String> environment,
            Set<String> modules,
            Set<String> plugins,
            DistributionType distributionType,
            Set<FeatureFlag> features
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
        }

        public LocalClusterSpec getCluster() {
            return cluster;
        }

        public String getName() {
            return name == null ? cluster.getName() + "-" + cluster.getNodes().indexOf(this) : name;
        }

        public Version getVersion() {
            return version;
        }

        public List<User> getUsers() {
            return cluster.getUsers();
        }

        public List<TextResource> getRolesFiles() {
            return cluster.getRoleFiles();
        }

        public DistributionType getDistributionType() {
            return distributionType;
        }

        public Set<String> getModules() {
            return modules;
        }

        public Set<String> getPlugins() {
            return plugins;
        }

        public Set<FeatureFlag> getFeatures() {
            return features;
        }

        public boolean isSecurityEnabled() {
            return Boolean.parseBoolean(
                resolveSettings().getOrDefault("xpack.security.enabled", getVersion().onOrAfter("8.0.0") ? "true" : "false")
            );
        }

        public boolean isSettingTrue(String setting) {
            return Boolean.parseBoolean(resolveSettings().getOrDefault(setting, "false"));
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
            settingsProviders.forEach(p -> resolvedSettings.putAll(p.get(this)));
            resolvedSettings.putAll(settings);
            return resolvedSettings;
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
    }
}
