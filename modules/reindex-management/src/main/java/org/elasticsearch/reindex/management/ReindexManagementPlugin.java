/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.reindex.ReindexPlugin.REINDEX_RESILIENCE_ENABLED;

public class ReindexManagementPlugin extends Plugin implements ActionPlugin {

    public static final String CAPABILITY_REINDEX_MANAGEMENT_API = "reindex_management_api";

    @Override
    public List<ActionHandler> getActions() {
        if (REINDEX_RESILIENCE_ENABLED) {
            return List.of(
                new ActionHandler(TransportGetReindexAction.TYPE, TransportGetReindexAction.class),
                new ActionHandler(TransportListReindexAction.TYPE, TransportListReindexAction.class),
                new ActionHandler(TransportCancelReindexAction.TYPE, TransportCancelReindexAction.class)
            );
        } else {
            return List.of();
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (REINDEX_RESILIENCE_ENABLED) {
            return List.of(
                new RestGetReindexAction(clusterSupportsFeature),
                new RestListReindexAction(clusterSupportsFeature),
                new RestCancelReindexAction(clusterSupportsFeature)
            );
        } else {
            return List.of();
        }
    }
}
