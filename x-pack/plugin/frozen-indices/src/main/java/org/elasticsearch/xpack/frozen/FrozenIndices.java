/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.action.TransportFreezeIndexAction;
import org.elasticsearch.xpack.frozen.rest.action.RestFreezeIndexAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class FrozenIndices extends Plugin implements ActionPlugin, EnginePlugin {

    private boolean transportClientMode;

    public FrozenIndices(Settings settings) {
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(FrozenEngine.INDEX_FROZEN)) {
            return Optional.of(FrozenEngine::new);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(FrozenEngine.INDEX_FROZEN);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (FrozenEngine.INDEX_FROZEN.get(indexModule.getSettings())) {
            indexModule.addSearchOperationListener(new FrozenEngine.ReacquireEngineSearcherListener());
        }
        super.onIndexModule(indexModule);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, FrozenIndicesFeatureSet.class));

        return modules;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(FreezeIndexAction.INSTANCE, TransportFreezeIndexAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.singletonList(new RestFreezeIndexAction());
    }
}
