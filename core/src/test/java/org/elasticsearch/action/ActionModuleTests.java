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

package org.elasticsearch.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.startsWith;

public class ActionModuleTests extends ESTestCase {

    public void testSetupRestHandlerContainsKnownBuiltin() {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        UsageService usageService = new UsageService(settings.getSettings());
        ActionModule actionModule = new ActionModule(false, settings.getSettings(), new IndexNameExpressionResolver(Settings.EMPTY),
                settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), null, emptyList(),
                emptyList(), null, null, usageService);
        actionModule.initRestHandlers(null);
        // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            actionModule.getRestController().registerHandler(Method.GET, "/", null));
        assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() throws IOException {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                    IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                    IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
                return singletonList(new RestMainAction(settings, restController));
            }
        };
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService(settings.getSettings());
            ActionModule actionModule = new ActionModule(false, settings.getSettings(), new IndexNameExpressionResolver(Settings.EMPTY),
                    settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                    singletonList(dupsMainAction), singletonList(dupsMainAction), null, null, usageService);
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testPluginCanRegisterRestHandler() {
        class FakeHandler implements RestHandler {
            FakeHandler(RestController restController) {
                restController.registerHandler(Method.GET, "/_dummy", this);
            }
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            }
        }
        ActionPlugin registersFakeHandler = new ActionPlugin() {
            @Override
            public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                    IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                    IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
                return singletonList(new FakeHandler(restController));
            }
        };

        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService(settings.getSettings());
            ActionModule actionModule = new ActionModule(false, settings.getSettings(), new IndexNameExpressionResolver(Settings.EMPTY),
                    settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                    singletonList(registersFakeHandler), singletonList(registersFakeHandler), null, null, usageService);
            actionModule.initRestHandlers(null);
            // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
            Exception e = expectThrows(IllegalArgumentException.class, () ->
                actionModule.getRestController().registerHandler(Method.GET, "/_dummy", null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_dummy] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }
}
