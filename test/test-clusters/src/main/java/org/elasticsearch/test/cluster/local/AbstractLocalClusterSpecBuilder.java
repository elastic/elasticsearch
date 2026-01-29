/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class AbstractLocalClusterSpecBuilder<T extends ElasticsearchCluster> extends AbstractLocalSpecBuilder<
    LocalClusterSpecBuilder<T>> implements LocalClusterSpecBuilder<T> {

    private String name = "test-cluster";
    private boolean shared = false;
    private final List<DefaultLocalNodeSpecBuilder> nodeBuilders = new ArrayList<>();
    private final List<User> users = new ArrayList<>();
    private final List<Resource> roleFiles = new ArrayList<>();
    private final List<Supplier<LocalClusterConfigProvider>> lazyConfigProviders = new ArrayList<>();

    public AbstractLocalClusterSpecBuilder() {
        super(null);
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> apply(LocalClusterConfigProvider configProvider) {
        configProvider.apply(this);
        return this;
    }

    @Override
    public LocalClusterSpecBuilder<T> apply(Supplier<LocalClusterConfigProvider> configProvider) {
        lazyConfigProviders.add(configProvider);
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> nodes(int nodes) {
        if (nodes < nodeBuilders.size()) {
            throw new IllegalArgumentException(
                "Cannot shrink cluster to " + nodes + ". " + nodeBuilders.size() + " nodes already configured"
            );
        }

        int newNodes = nodes - nodeBuilders.size();
        for (int i = 0; i < newNodes; i++) {
            nodeBuilders.add(new DefaultLocalNodeSpecBuilder(this));
        }

        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> withNode(Consumer<? super LocalNodeSpecBuilder> config) {
        DefaultLocalNodeSpecBuilder builder = new DefaultLocalNodeSpecBuilder(this);
        config.accept(builder);
        nodeBuilders.add(builder);
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> node(int index, Consumer<? super LocalNodeSpecBuilder> config) {
        try {
            DefaultLocalNodeSpecBuilder builder = nodeBuilders.get(index);
            config.accept(builder);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                "No node at index + " + index + " exists. Only " + nodeBuilders.size() + " nodes have been configured"
            );
        }
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> user(String username, String password) {
        this.users.add(new User(username, password));
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> user(String username, String password, String role, boolean operator) {
        this.users.add(new User(username, password, role, operator));
        return this;
    }

    @Override
    public AbstractLocalClusterSpecBuilder<T> rolesFile(Resource rolesFile) {
        this.roleFiles.add(rolesFile);
        return this;
    }

    @Override
    public LocalClusterSpecBuilder<T> shared(Boolean isShared) {
        if (Integer.parseInt(System.getProperty("tests.max.parallel.forks")) > 1) {
            String taskPath = System.getProperty("tests.task");
            String project = taskPath.substring(0, taskPath.lastIndexOf(':'));
            String taskName = taskPath.substring(taskPath.lastIndexOf(':') + 1);

            throw new IllegalStateException(
                "Parallel test execution is not supported for shared clusters. Configure the build script for project '"
                    + project
                    + "':\n\n"
                    + "tasks.named('"
                    + taskName
                    + "') {\n"
                    + "  maxParallelForks = 1\n"
                    + "}"
            );
        }
        this.shared = isShared;
        return this;
    }

    protected LocalClusterSpec buildClusterSpec() {
        // Apply lazily provided configuration
        lazyConfigProviders.forEach(s -> s.get().apply(this));

        List<User> clusterUsers = users.isEmpty() ? List.of(User.DEFAULT_USER) : users;
        LocalClusterSpec clusterSpec = new LocalClusterSpec(name, clusterUsers, roleFiles, shared);
        List<LocalNodeSpec> nodeSpecs;

        if (nodeBuilders.isEmpty()) {
            // No node-specific configuration so assume a single-node cluster
            nodeSpecs = List.of(new DefaultLocalNodeSpecBuilder(this).build(clusterSpec, 0));
        } else {
            AtomicInteger nodeIndex = new AtomicInteger(0);
            nodeSpecs = nodeBuilders.stream().map(node -> node.build(clusterSpec, nodeIndex.getAndIncrement())).toList();
        }

        clusterSpec.setNodes(nodeSpecs);
        clusterSpec.validate();

        return clusterSpec;
    }

    public static class DefaultLocalNodeSpecBuilder extends AbstractLocalSpecBuilder<LocalNodeSpecBuilder> implements LocalNodeSpecBuilder {
        private String name;
        private boolean unsetName = false;

        protected DefaultLocalNodeSpecBuilder(AbstractLocalSpecBuilder<?> parent) {
            super(parent);
        }

        @Override
        public DefaultLocalNodeSpecBuilder name(String name) {
            if (unsetName) {
                throw new IllegalStateException("Cannot set name when 'withoutName()` has been used");
            }
            this.name = Objects.requireNonNull(
                name,
                "Name cannot be set to null. Consider using '.withoutName()' method if you need node without explicitly set name"
            );
            return this;
        }

        @Override
        public DefaultLocalNodeSpecBuilder withoutName() {
            if (name != null) {
                throw new IllegalStateException("Cannot use 'withoutName()', because name has been set for the node");
            }
            this.unsetName = true;
            return this;
        }

        private String resolveName(LocalClusterSpec cluster, int nodeIndex) {
            if (unsetName) {
                return null;
            }
            return name == null ? cluster.getName() + "-" + nodeIndex : name;
        }

        private LocalNodeSpec build(LocalClusterSpec cluster, int nodeIndex) {

            return new LocalNodeSpec(
                cluster,
                resolveName(cluster, nodeIndex),
                Optional.ofNullable(getVersion()).orElse(Version.CURRENT),
                getSettingsProviders(),
                getSettings(),
                getEnvironmentProviders(),
                getEnvironment(),
                getModules(),
                getPlugins(),
                Optional.ofNullable(getDistributionType()).orElse(DistributionType.INTEG_TEST),
                getFeatures(),
                getKeystoreProviders(),
                getKeystoreSettings(),
                getKeystoreFiles(),
                getKeystorePassword(),
                getExtraConfigFiles(),
                getSystemPropertyProviders(),
                getSystemProperties(),
                getJvmArgs(),
                Optional.ofNullable(getConfigDirSupplier()).map(Supplier::get).orElse(null)
            );
        }
    }
}
