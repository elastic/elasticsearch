/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.logstash;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestDeleteAction;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LogstashPlugin extends Plugin implements SystemIndexPlugin, ActionPlugin {

    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singleton(new SystemIndexDescriptor(".logstash*", "Logstash system indices for storing pipelines"));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.unmodifiableList(Arrays.asList(
            new LogstashWrappedRestHandler(new RestGetAction()),
            new LogstashWrappedRestHandler(new RestMultiGetAction(settings)),
            new LogstashWrappedRestHandler(new RestIndexAction()),
            new LogstashWrappedRestHandler(new RestDeleteAction()),
            new LogstashWrappedRestHandler(new RestSearchAction()),
            new LogstashWrappedRestHandler(new RestSearchScrollAction()),
            new LogstashWrappedRestHandler(new RestClearScrollAction())
        ));
    }

    static class LogstashWrappedRestHandler extends BaseRestHandler.Wrapper {

        private final List<String> allowedIndexPatterns = Collections.singletonList(".logstash");

        LogstashWrappedRestHandler(BaseRestHandler delegate) {
            super(delegate);
        }

        @Override
        public String getName() {
            return "logstash_" + super.getName();
        }

        @Override
        public List<Route> routes() {
            return super.routes().stream().map(route -> new Route(route.getMethod(), "/_logstash" + route.getPath()))
                .collect(Collectors.toUnmodifiableList());
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            client.threadPool().getThreadContext().allowSystemIndexAccess(allowedIndexPatterns);
            return super.prepareRequest(request, client);
        }
    }
}
