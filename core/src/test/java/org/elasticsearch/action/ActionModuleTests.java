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

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.TransportMainAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.ClientActionPlugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;

public class ActionModuleTests extends ESTestCase {

    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(ActionModule.registerActions(emptyList()),
                hasEntry(MainAction.INSTANCE.name(), new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class)));
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class));
            }
        };

        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.registerClientActions(singletonList(dupsMainAction)));
        assertEquals("clientAction for name [" + MainAction.NAME + "] already registered", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> ActionModule.registerActions(singletonList(dupsMainAction)));
        assertEquals("action for name [" + MainAction.NAME + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
        class FakeTransportAction extends TransportAction<FakeRequest, ActionResponse> {
            protected FakeTransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {
                super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, taskManager);
            }

            @Override
            protected void doExecute(FakeRequest request, ActionListener<ActionResponse> listener) {
            }
        }
        class FakeAction extends GenericAction<FakeRequest, ActionResponse> {
            protected FakeAction() {
                super("fake");
            }

            @Override
            public ActionResponse newResponse() {
                return null;
            }
        }
        FakeAction action = new FakeAction();
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(action, FakeTransportAction.class));
            }
        };

        assertThat(ActionModule.registerActions(singletonList(registersFakeAction)),
            hasEntry("fake", new ActionHandler<>(action, FakeTransportAction.class)));
    }

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

    /**
     * This test ensures that no Actions are added to core without also adding an associated Client Action. If you find this
     * test failing, it is likely because the {@link ActionPlugin#getActions()} and {@link ClientActionPlugin#getClientActions()}
     * are not returning the same list of {@link GenericAction}. Consult your {@link ActionPlugin} to ensure both methods
     * are overridden and contain the same {@link GenericAction}'s.
     */
    public void testDefaultActionsAndClientActionsContainTheSameKeys() {
        // Do not add any extra actions to assert they are the same
        List<GenericAction> genericActionsFromActions = ActionModule.registerActions(emptyList()).values()
            .stream()
            .map(a->a.getAction())
            .collect(Collectors.toList());
        // we aren't checking 2 empty lists
        assertNotEquals(genericActionsFromActions.size(), 0);
        // the client action list is the same as the generic action list
        assertThat(genericActionsFromActions,
            containsInAnyOrder(ActionModule.registerClientActions(emptyList()).toArray(new GenericAction[0])));
    }

    /**
     * This test ensures that the generation of additional actions and client actions above the base set included in core
     * will always contain the same set of items. This ensures that the client can bind to {@link ActionModule#getClientActions()}
     * and know that the same associated actions are registered for routing on the server.
     */
    public void testAddedActionsAndClientActionsContainTheSameKeys() {

        class FakeAction extends GenericAction<ActionRequest, ActionResponse> {
            private FakeAction() {
                super("fake");
            }

            @Override
            public ActionResponse newResponse() {
                return null;
            }
        }

        class FakeTransportAction extends TransportAction<ActionRequest, ActionResponse> {
            protected FakeTransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {
                super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, taskManager);
            }

            @Override
            protected void doExecute(ActionRequest request, ActionListener<ActionResponse> listener) {
            }
        }
        FakeAction genericAction = new FakeAction();
        ActionPlugin fakeActionPlugin = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(genericAction, FakeTransportAction.class));
            }
        };

        ClientActionPlugin fakeClientActionPlugin = new ClientActionPlugin() {
            @Override
            public List<GenericAction<? extends ActionRequest, ? extends ActionResponse>> getClientActions() {
                return Collections.singletonList(genericAction);
            }
        };

        // Add the actions in to make sure they are the same coming out
        List<GenericAction> genericActionsFromActions = ActionModule.registerActions(singletonList(fakeActionPlugin)).values()
            .stream()
            .map(a->a.getAction())
            .collect(Collectors.toList());
        // we aren't checking 2 empty lists
        assertNotEquals(genericActionsFromActions.size(), 0);
        // the actions size is not the same as a default action list, which should have 1 less
        assertEquals(genericActionsFromActions.size(), ActionModule.registerActions(emptyList()).size() + 1);
        // the client action list is the same as the generic action list
        assertThat(genericActionsFromActions,
            containsInAnyOrder(ActionModule.registerClientActions(singletonList(fakeClientActionPlugin)).toArray(new GenericAction[0])));
    }
}
