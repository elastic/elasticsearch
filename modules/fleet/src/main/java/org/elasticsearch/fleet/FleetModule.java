/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.fleet;

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

public class FleetModule extends Plugin implements SystemIndexPlugin {


    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(".fleet-outputs*", "Configuration of fleet outputs"),
            new SystemIndexDescriptor(".fleet-policies*", "Policies and enrollment keys"),
            new SystemIndexDescriptor(".fleet-agents*", "Agents and agent checkins")
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
