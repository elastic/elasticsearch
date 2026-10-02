/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation.plugin;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationSettings.TEST_DEPRECATED_SETTING_TRUE1;
import static org.elasticsearch.xpack.deprecation.DeprecationSettings.TEST_DEPRECATED_SETTING_TRUE2;
import static org.elasticsearch.xpack.deprecation.DeprecationSettings.TEST_NOT_DEPRECATED_SETTING;

/**
 * Adds {@link TestDeprecationHeaderRestAction} for testing deprecation requests via HTTP.
 */
public class TestDeprecationPlugin extends Plugin implements ActionPlugin, SearchPlugin, RepositoryPlugin {

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(TestRepositoryDeprecationSetupAction.TYPE, TestRepositoryDeprecationSetupAction.TransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new TestDeprecationHeaderRestAction(restHandlersServices.settings()),
            new TestRepositoryDeprecationSetupAction.RestAction()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(TEST_DEPRECATED_SETTING_TRUE1, TEST_DEPRECATED_SETTING_TRUE2, TEST_NOT_DEPRECATED_SETTING);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(
            new QuerySpec<>(TestDeprecatedQueryBuilder.NAME, TestDeprecatedQueryBuilder::new, TestDeprecatedQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics,
        SnapshotMetrics snapshotMetrics
    ) {
        return Map.ofEntries(TestRepositoryDeprecationSetupAction.invalidRepositoryEntry());
    }

}
