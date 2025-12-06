/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
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
    public List<ActionHandler> getActions() {
        return entSearchPlugin.getActions();
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
        return entSearchPlugin.getRestHandlers(
            settings,
            namedWriteableRegistry,
            restController,
            clusterSettings,
            indexScopedSettings,
            settingsFilter,
            indexNameExpressionResolver,
            nodesInCluster,
            clusterSupportsFeature
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return entSearchPlugin.createComponents(services);
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
