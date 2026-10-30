/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class LocalStateRankVectors extends LocalStateCompositeXPackPlugin {

    private final RankVectorsPlugin rankVectorsPlugin;
    private final XPackLicenseState licenseState = new XPackLicenseState(
        System::currentTimeMillis,
        new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
    );

    public LocalStateRankVectors(Settings settings) {
        super(settings, null);
        LocalStateRankVectors thisVar = this;
        rankVectorsPlugin = new RankVectorsPlugin() {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return rankVectorsPlugin.getMappers();
    }

    @Override
    public List<SearchPlugin.QuerySpec<?>> getQueries() {
        return rankVectorsPlugin.getQueries();
    }

    @Override
    public List<SearchPlugin.RetrieverSpec<?>> getRetrievers() {
        return rankVectorsPlugin.getRetrievers();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(super.getSettings());
        settings.addAll(rankVectorsPlugin.getSettings());
        return settings;
    }

    @Override
    public List<ActionPlugin.ActionHandler> getActions() {
        List<ActionPlugin.ActionHandler> actions = new ArrayList<>(super.getActions());
        actions.addAll(rankVectorsPlugin.getActions());
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        ActionPlugin.RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>(super.getRestHandlers(restHandlersServices, nodesInCluster, clusterSupportsFeature));
        handlers.addAll(rankVectorsPlugin.getRestHandlers(restHandlersServices, nodesInCluster, clusterSupportsFeature));
        return handlers;
    }
}
