/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class ActionModuleTests extends ESTestCase {
    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(
            ActionModule.setupActions(emptyList()),
            hasEntry(TransportNodesInfoAction.TYPE.name(), new ActionHandler(TransportNodesInfoAction.TYPE, TransportNodesInfoAction.class))
        );
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler> getActions() {
                return singletonList(new ActionHandler(TransportNodesInfoAction.TYPE, TransportNodesInfoAction.class));
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(singletonList(dupsMainAction)));
        assertEquals("action for name [" + TransportNodesInfoAction.TYPE.name() + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends LegacyActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
        class FakeTransportAction extends TransportAction<FakeRequest, ActionResponse> {
            protected FakeTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
                super(actionName, actionFilters, taskManager, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            }

            @Override
            protected void doExecute(Task task, FakeRequest request, ActionListener<ActionResponse> listener) {}
        }
        final var action = new ActionType<>("fake");
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler> getActions() {
                return singletonList(new ActionHandler(action, FakeTransportAction.class));
            }
        };
        assertThat(
            ActionModule.setupActions(singletonList(registersFakeAction)),
            hasEntry("fake", new ActionHandler(action, FakeTransportAction.class))
        );
    }

    public void testSetupRestHandlerContainsKnownBuiltin() {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        UsageService usageService = new UsageService();
        ActionModule actionModule = new ActionModule(
            settings.getSettings(),
            TestIndexNameExpressionResolver.newInstance(),
            null,
            settings.getIndexScopedSettings(),
            settings.getClusterSettings(),
            settings.getSettingsFilter(),
            null,
            emptyList(),
            null,
            null,
            usageService,
            null,
            TelemetryProvider.NOOP,
            mock(ClusterService.class),
            null,
            List.of(),
            List.of(),
            RestExtension.allowAll(),
            new IncrementalBulkService(null, null),
            TestProjectResolvers.alwaysThrow()
        );
        actionModule.initRestHandlers(null, null);
        // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> actionModule.getRestController().registerHandler(new RestHandler() {
                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, "/_nodes"));
                }
            })
        );
        assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_nodes] for method: GET"));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() throws IOException {
        ActionPlugin dupsMainAction = new ActionPlugin() {
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
                return singletonList(new RestNodesInfoAction(new SettingsFilter(emptyList())) {

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
            ActionModule actionModule = new ActionModule(
                settings.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                null,
                settings.getIndexScopedSettings(),
                settings.getClusterSettings(),
                settings.getSettingsFilter(),
                threadPool,
                singletonList(dupsMainAction),
                null,
                null,
                usageService,
                null,
                TelemetryProvider.NOOP,
                mock(ClusterService.class),
                null,
                List.of(),
                List.of(),
                RestExtension.allowAll(),
                new IncrementalBulkService(null, null),
                TestProjectResolvers.alwaysThrow()
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null, null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_nodes] for method: GET"));
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
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}
        }
        ActionPlugin registersFakeHandler = new ActionPlugin() {
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
                return singletonList(new FakeHandler());
            }
        };

        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule = new ActionModule(
                settings.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                null,
                settings.getIndexScopedSettings(),
                settings.getClusterSettings(),
                settings.getSettingsFilter(),
                threadPool,
                singletonList(registersFakeHandler),
                null,
                null,
                usageService,
                null,
                TelemetryProvider.NOOP,
                mock(ClusterService.class),
                null,
                List.of(),
                List.of(),
                RestExtension.allowAll(),
                new IncrementalBulkService(null, null),
                TestProjectResolvers.alwaysThrow()
            );
            actionModule.initRestHandlers(null, null);
            // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> actionModule.getRestController().registerHandler(new RestHandler() {
                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}

                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(GET, "/_dummy"));
                    }
                })
            );
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_dummy] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void test3rdPartyHandlerIsNotInstalled() {
        Settings settings = Settings.builder().put("xpack.security.enabled", false).put("path.home", createTempDir()).build();

        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ActionPlugin secPlugin = new SecPlugin(true, false);
        try {
            UsageService usageService = new UsageService();

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> new ActionModule(
                    settingsModule.getSettings(),
                    TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                    null,
                    settingsModule.getIndexScopedSettings(),
                    settingsModule.getClusterSettings(),
                    settingsModule.getSettingsFilter(),
                    threadPool,
                    Arrays.asList(secPlugin),
                    null,
                    null,
                    usageService,
                    null,
                    null,
                    mock(ClusterService.class),
                    null,
                    List.of(),
                    List.of(),
                    RestExtension.allowAll(),
                    new IncrementalBulkService(null, null),
                    TestProjectResolvers.alwaysThrow()
                )
            );
            assertThat(
                e.getMessage(),
                Matchers.equalTo(
                    "The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to "
                        + "install a custom REST interceptor. This functionality is not available to external plugins."
                )
            );
        } finally {
            threadPool.shutdown();
        }
    }

    public void test3rdPartyRestControllerIsNotInstalled() {
        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ActionPlugin secPlugin = new SecPlugin(false, true);
        try {
            UsageService usageService = new UsageService();

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> new ActionModule(
                    settingsModule.getSettings(),
                    TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                    null,
                    settingsModule.getIndexScopedSettings(),
                    settingsModule.getClusterSettings(),
                    settingsModule.getSettingsFilter(),
                    threadPool,
                    List.of(secPlugin),
                    null,
                    null,
                    usageService,
                    null,
                    TelemetryProvider.NOOP,
                    mock(ClusterService.class),
                    null,
                    List.of(),
                    List.of(),
                    RestExtension.allowAll(),
                    new IncrementalBulkService(null, null),
                    TestProjectResolvers.alwaysThrow()
                )
            );
            assertThat(
                e.getMessage(),
                Matchers.equalTo(
                    "The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to install a custom REST controller."
                        + " This functionality is not available to external plugins."
                )
            );
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
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}
    }

    class SecPlugin implements ActionPlugin, RestServerActionPlugin {
        private final boolean installInterceptor;
        private final boolean installController;

        SecPlugin(boolean installInterceptor, boolean installController) {
            this.installInterceptor = installInterceptor;
            this.installController = installController;
        }

        @Override
        public RestInterceptor getRestHandlerInterceptor(ThreadContext threadContext) {
            if (installInterceptor) {
                return (request, channel, targetHandler, listener) -> listener.onResponse(true);
            } else {
                return null;
            }
        }

        @Override
        public RestController getRestController(
            RestInterceptor interceptor,
            NodeClient client,
            CircuitBreakerService circuitBreakerService,
            UsageService usageService,
            TelemetryProvider telemetryProvider
        ) {
            if (installController) {
                return new RestController(interceptor, client, circuitBreakerService, usageService, telemetryProvider);
            } else {
                return null;
            }
        }
    }
}
