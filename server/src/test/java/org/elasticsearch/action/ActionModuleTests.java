/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;

public class ActionModuleTests extends ESTestCase {
    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(ActionModule.setupActions(emptyList()),
                hasEntry(MainAction.INSTANCE.name(), new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class)));
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class));
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(singletonList(dupsMainAction)));
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
            protected FakeTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
                super(actionName, actionFilters, taskManager);
            }

            @Override
            protected void doExecute(Task task, FakeRequest request, ActionListener<ActionResponse> listener) {
            }
        }
        class FakeAction extends ActionType<ActionResponse> {
            protected FakeAction() {
                super("fake", null);
            }
        }
        FakeAction action = new FakeAction();
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(action, FakeTransportAction.class));
            }
        };
        assertThat(ActionModule.setupActions(singletonList(registersFakeAction)),
                hasEntry("fake", new ActionHandler<>(action, FakeTransportAction.class)));
    }

    public void testSetupRestHandlerContainsKnownBuiltin() {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        UsageService usageService = new UsageService();
        ActionModule actionModule = new ActionModule(settings.getSettings(),
            TestIndexNameExpressionResolver.newInstance(),
            settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), null, emptyList(), null,
            null, usageService, null);
        actionModule.initRestHandlers(null);
        // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            actionModule.getRestController().registerHandler(new RestHandler() {
                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, "/"));
                }
            }));
        assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() throws IOException {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                    IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                    IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
                return singletonList(new RestMainAction() {

                    @Override
                    public String getName() {
                        return "duplicated_" + super.getName();
                    }

                });
            }
        };
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule = new ActionModule(settings.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                singletonList(dupsMainAction), null, null, usageService, null);
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testPluginCanRegisterRestHandler() {
        class FakeHandler implements RestHandler {
            @Override
            public List<Route> routes() {
                return List.of(new Route(GET, "/_dummy"));
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
                return singletonList(new FakeHandler());
            }
        };

        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule = new ActionModule(settings.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                singletonList(registersFakeHandler), null, null, usageService, null);
            actionModule.initRestHandlers(null);
            // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
            Exception e = expectThrows(IllegalArgumentException.class, () ->
                actionModule.getRestController().registerHandler(new RestHandler() {
                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                    }

                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(GET, "/_dummy"));
                    }
                }));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_dummy] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void test3rdPartyHandlerIsNotInstalled() {
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", false)
            .put("path.home", createTempDir())
            .build();

        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ActionPlugin secPlugin = new SecPlugin();
        try {
            UsageService usageService = new UsageService();

            Exception e = expectThrows(IllegalArgumentException.class, () ->
                new ActionModule(settingsModule.getSettings(),
                    TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                    settingsModule.getIndexScopedSettings(), settingsModule.getClusterSettings(), settingsModule.getSettingsFilter(),
                    threadPool, Arrays.asList(secPlugin), null, null, usageService, null)
            );
            assertThat(e.getMessage(), Matchers.equalTo("The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to " +
                "install a custom REST wrapper. This functionality is not available anymore."));
        } finally {
            threadPool.shutdown();
        }
    }

    class FakeHandler implements RestHandler {
        @Override
        public List<Route> routes() {
            return singletonList(new Route(GET, "/_dummy"));
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        }
    }

    class SecPlugin implements ActionPlugin {
        @Override
        public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
            return UnaryOperator.identity();
        }
    };
}
