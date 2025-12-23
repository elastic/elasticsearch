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
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ReindexManagementPlugin extends Plugin implements ActionPlugin {

    @Override
    public Collection<ActionHandler> getActions() {
        return ReindexPlugin.REINDEX_RESILIENCE_ENABLED
            ? List.of(new ActionHandler(TransportCancelReindexAction.TYPE, TransportCancelReindexAction.class))
            : List.of();
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        final Settings settings,
        final NamedWriteableRegistry namedWriteableRegistry,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster,
        final Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return ReindexPlugin.REINDEX_RESILIENCE_ENABLED ? List.of(new RestCancelReindexAction(clusterSupportsFeature)) : List.of();
    }
}
