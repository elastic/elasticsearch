/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;
import org.elasticsearch.xpack.entsearch.engine.action.GetEngineAction;
import org.elasticsearch.xpack.entsearch.engine.action.PutEngineAction;
import org.elasticsearch.xpack.entsearch.engine.action.RestGetEngineAction;
import org.elasticsearch.xpack.entsearch.engine.action.RestPutEngineAction;
import org.elasticsearch.xpack.entsearch.engine.action.TransportGetEngineAction;
import org.elasticsearch.xpack.entsearch.engine.action.TransportPutEngineAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class EnterpriseSearch extends Plugin implements ActionPlugin, SystemIndexPlugin {
    public static final String ENGINE_API_ENDPOINT = "_engine";

    private static final Logger logger = LogManager.getLogger(EnterpriseSearch.class);

    public static final String FEATURE_NAME = "ent_search";

    private final boolean enabled;

    public EnterpriseSearch(Settings settings) {
        this.enabled = XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings);
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return Collections.emptyList();
        }
        return List.of(
            new ActionPlugin.ActionHandler<>(GetEngineAction.INSTANCE, TransportGetEngineAction.class),
            new ActionHandler<>(PutEngineAction.INSTANCE, TransportPutEngineAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {

        if (enabled == false) {
            return Collections.emptyList();
        }
        return List.of(new RestGetEngineAction(), new RestPutEngineAction());
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService
    ) {
        if (enabled == false) {
            return Collections.emptyList();
        }
        // TODO Don't implement this as a component, instantiate it when needed
        final EngineIndexService engineService = new EngineIndexService(
            client,
            clusterService,
            namedWriteableRegistry,
            // TODO We need to use use a real BigArrays which recycles pages here
            BigArrays.NON_RECYCLING_INSTANCE
        );
        return Collections.singletonList(engineService);
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Arrays.asList(EngineIndexService.getSystemIndexDescriptor());
    }

    @Override
    public String getFeatureName() {
        return FEATURE_NAME;
    }

    @Override
    public String getFeatureDescription() {
        return "Manages configuration for Enterprise Search features";
    }

}
