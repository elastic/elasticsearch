/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.entityanalytics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.entityanalytics.action.EntityRiskScoringAction;
import org.elasticsearch.xpack.entityanalytics.rest.EntityRiskScoringRestHandler;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class EntityRiskPlugin extends Plugin implements ActionPlugin {

    public static final LicensedFeature.Momentary ENTITY_ANALYTICS_FEATURE =
        LicensedFeature.momentary(null, "entity_analytics", License.OperationMode.PLATINUM);

    protected final boolean enabled;

    public EntityRiskPlugin(Settings settings) {
        this.enabled = XPackSettings.ENTITY_ANALYTICS_ENABLED.get(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(EntityRiskScoringAction.INSTANCE, EntityRiskScoringAction.TransportEntityRiskScoringAction.class)
        );
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
        if (enabled) {
            return singletonList(new EntityRiskScoringRestHandler());
        }
        return emptyList();
    }
}
