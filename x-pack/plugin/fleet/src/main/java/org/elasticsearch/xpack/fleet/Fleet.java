/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.reindex.RestDeleteByQueryAction;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.admin.indices.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestRefreshAction;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.rest.action.document.RestDeleteAction;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.rest.action.document.RestUpdateAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Fleet extends Plugin implements SystemIndexPlugin {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(".fleet-servers*", "Configuration of fleet servers"),
            new SystemIndexDescriptor(".fleet-policies*", "Policies and enrollment keys"),
            new SystemIndexDescriptor(".fleet-agents*", "Agents and agent checkins"),
            new SystemIndexDescriptor(".fleet-actions*", "Fleet actions")
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
        // TODO need to lock down access to just Fleet indices
        return List.of(
            new FleetWrappedRestHandler(new RestCreateIndexAction()),

            new FleetWrappedRestHandler(new RestIndexAction()),
            new FleetWrappedRestHandler(new RestGetAction()),
            new FleetWrappedRestHandler(new RestUpdateAction()),
            new FleetWrappedRestHandler(new CreateHandler()),
            new FleetWrappedRestHandler(new AutoIdHandler(nodesInCluster)),

            new FleetWrappedRestHandler(new RestSearchAction()),
            new FleetWrappedRestHandler(new RestSearchScrollAction()),
            new FleetWrappedRestHandler(new RestClearScrollAction()),

            new FleetWrappedRestHandler(new RestBulkAction(settings)),
            new FleetWrappedRestHandler(new RestDeleteAction()),
            new FleetWrappedRestHandler(new RestDeleteByQueryAction()),
            new FleetWrappedRestHandler(new RestRefreshAction())
        );

    }

    static class FleetWrappedRestHandler extends BaseRestHandler.Wrapper {

        FleetWrappedRestHandler(BaseRestHandler delegate) {
            super(delegate);
        }

        @Override
        public String getName() {
            return "fleet_" + super.getName();
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return true;
        }

        @Override
        public List<Route> routes() {
            return super.routes().stream()
                .map(route -> new Route(route.getMethod(), "/_fleet" + route.getPath()))
                .collect(Collectors.toUnmodifiableList());
        }
    }
}
