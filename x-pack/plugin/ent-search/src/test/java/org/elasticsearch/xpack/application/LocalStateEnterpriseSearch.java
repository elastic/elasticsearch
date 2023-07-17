/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class LocalStateEnterpriseSearch extends LocalStateCompositeXPackPlugin {

    private final EnterpriseSearch entSearchPlugin;
    private final XPackLicenseState licenseState;

    public LocalStateEnterpriseSearch(Settings settings, Path configPath) {
        super(settings, configPath);
        this.licenseState = mockLicenseState(true);

        this.entSearchPlugin = new EnterpriseSearch(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };

        plugins.add(entSearchPlugin);
    }

    private MockLicenseState mockLicenseState(boolean isLicensed) {
        MockLicenseState mock = Mockito.mock(MockLicenseState.class);
        Mockito.when(mock.copyCurrentLicenseState()).thenReturn(mock);
        Mockito.when(mock.isAllowed(Mockito.any())).thenReturn(isLicensed);
        Mockito.when(mock.isAllowedByLicense(Mockito.any())).thenReturn(isLicensed);
        Mockito.when(mock.isActive()).thenReturn(isLicensed);
        return mock;
    }

    @Override
    protected XPackLicenseState getLicenseState() {
        return licenseState;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return entSearchPlugin.getActions();
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
        return entSearchPlugin.getRestHandlers(
            settings,
            restController,
            clusterSettings,
            indexScopedSettings,
            settingsFilter,
            indexNameExpressionResolver,
            nodesInCluster
        );
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
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        return entSearchPlugin.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier,
            tracer,
            allocationService,
            indicesService
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return entSearchPlugin.getSystemIndexDescriptors(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return entSearchPlugin.getSettings();
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return entSearchPlugin.getQueries();
    }

}
