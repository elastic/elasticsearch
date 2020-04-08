/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.kibana;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
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
import org.elasticsearch.rest.action.admin.indices.RestGetAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestGetIndicesAction;
import org.elasticsearch.rest.action.admin.indices.RestIndexPutAliasAction;
import org.elasticsearch.rest.action.admin.indices.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.rest.action.document.RestDeleteAction;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.rest.action.document.RestUpdateAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    public static final Setting<List<String>> KIBANA_INDEX_NAMES_SETTING = Setting.listSetting(
        "kibana.system_indices",
        List.of(".kibana*", ".reporting"),
        Function.identity(),
        Property.NodeScope
    );

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return KIBANA_INDEX_NAMES_SETTING.get(settings)
            .stream()
            .map(pattern -> new SystemIndexDescriptor(pattern, "System index used by kibana"))
            .collect(Collectors.toUnmodifiableList());
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
        // TODO need to figure out what subset of system indices Kibana should have access to via these APIs
        return List.of(
            // Based on https://github.com/elastic/kibana/issues/49764
            // apis needed to perform migrations... ideally these will go away
            new KibanaWrappedRestHandler(new RestCreateIndexAction()),
            new KibanaWrappedRestHandler(new RestGetAliasesAction()),
            new KibanaWrappedRestHandler(new RestIndexPutAliasAction()),
            new KibanaWrappedRestHandler(new RestRefreshAction()),

            // apis needed to access saved objects
            new KibanaWrappedRestHandler(new RestGetAction()),
            new KibanaWrappedRestHandler(new RestMultiGetAction(settings)),
            new KibanaWrappedRestHandler(new RestSearchAction()),
            new KibanaWrappedRestHandler(new RestBulkAction(settings)),
            new KibanaWrappedRestHandler(new RestDeleteAction()),
            new KibanaWrappedRestHandler(new RestDeleteByQueryAction()),

            // api used for testing
            new KibanaWrappedRestHandler(new RestUpdateSettingsAction()),

            // apis used specifically by reporting
            new KibanaWrappedRestHandler(new RestGetIndicesAction()),
            new KibanaWrappedRestHandler(new RestIndexAction()),
            new KibanaWrappedRestHandler(new CreateHandler()),
            new KibanaWrappedRestHandler(new AutoIdHandler(nodesInCluster)),
            new KibanaWrappedRestHandler(new RestUpdateAction()),
            new KibanaWrappedRestHandler(new RestSearchScrollAction()),
            new KibanaWrappedRestHandler(new RestClearScrollAction())
        );

    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(KIBANA_INDEX_NAMES_SETTING);
    }

    static class KibanaWrappedRestHandler extends BaseRestHandler.Wrapper {

        KibanaWrappedRestHandler(BaseRestHandler delegate) {
            super(delegate);
        }

        @Override
        public String getName() {
            return "kibana_" + super.getName();
        }

        @Override
        public List<Route> routes() {
            return super.routes().stream()
                .map(route -> new Route(route.getMethod(), "/_kibana" + route.getPath()))
                .collect(Collectors.toUnmodifiableList());
        }
    }
}
