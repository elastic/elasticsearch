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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.rest.DefaultRestInterceptorChain;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestContentTypePolicy;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestInterceptorChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class ActionModuleTests extends ESTestCase {
    Environment testEnv;

    @Before
    public void setupEnv() {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        testEnv = TestEnvironment.newEnvironment(settings);
    }

    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(
            ActionModule.setupActions(testEnv, emptyList()),
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(testEnv, singletonList(dupsMainAction)));
        assertEquals("action for name [" + TransportNodesInfoAction.TYPE.name() + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends UntypedActionRequest {
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
            ActionModule.setupActions(testEnv, singletonList(registersFakeAction)),
            hasEntry("fake", new ActionHandler(action, FakeTransportAction.class))
        );
    }

    public void testSetupRestHandlerContainsKnownBuiltin() {
        ActionModule actionModule = newActionModule(null, emptyList());
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
                RestHandlersServices restHandlersServices,
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
        try (var threadPool = new TestThreadPool(getTestName())) {
            ActionModule actionModule = newActionModule(threadPool, singletonList(dupsMainAction));
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null, null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_nodes] for method: GET"));
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
                RestHandlersServices restHandlersServices,
                Supplier<DiscoveryNodes> nodesInCluster,
                Predicate<NodeFeature> clusterSupportsFeature
            ) {
                return singletonList(new FakeHandler());
            }
        };

        try (var threadPool = new TestThreadPool(getTestName())) {
            ActionModule actionModule = newActionModule(threadPool, singletonList(registersFakeHandler));
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
        }
    }

    public void testInterceptorsAreSortedByOrder() {
        List<Integer> callOrder = new ArrayList<>();

        class OrderedInterceptor implements RestInterceptor {
            private final int order;

            OrderedInterceptor(int order) {
                this.order = order;
            }

            @Override
            public void intercept(RestInterceptorChain chain, ActionListener<Void> listener) {
                callOrder.add(order);
                chain.proceed(listener);
            }

            @Override
            public int order() {
                return order;
            }
        }

        List<RestInterceptor> sorted = new ArrayList<>(
            List.of(new OrderedInterceptor(42), new OrderedInterceptor(12), new OrderedInterceptor(123), new OrderedInterceptor(-111))
        );
        sorted.sort(Comparator.comparing(RestInterceptor::order));

        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        var chain = new DefaultRestInterceptorChain(
            request,
            mock(RestChannel.class),
            mock(RestHandler.class),
            mock(NodeClient.class),
            sorted
        );
        var future = new PlainActionFuture<Void>();
        chain.proceed(future);
        future.actionGet(10, TimeUnit.SECONDS);

        assertThat(callOrder, Matchers.equalTo(List.of(-111, 12, 42, 123)));
    }

    public void test3rdPartyRestInterceptorIsNotInstalled() {
        ActionPlugin secPlugin = new SecPlugin(true, false, false);
        try (var threadPool = new TestThreadPool(getTestName())) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> newActionModule(threadPool, Arrays.asList(secPlugin)));
            assertThat(
                e.getMessage(),
                Matchers.equalTo(
                    "The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to "
                        + "install a custom REST interceptor. This functionality is not available to external plugins."
                )
            );
        }
    }

    public void test3rdPartyRestContentTypePolicyIsNotInstalled() {
        ActionPlugin secPlugin = new SecPlugin(false, true, false);
        try (var threadPool = new TestThreadPool(getTestName())) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> newActionModule(threadPool, List.of(secPlugin)));
            assertThat(
                e.getMessage(),
                Matchers.equalTo(
                    "The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to install a custom REST content type policy."
                        + " This functionality is not available to external plugins."
                )
            );
        }
    }

    public void test3rdPartyRestControllerIsNotInstalled() {
        ActionPlugin secPlugin = new SecPlugin(false, false, true);
        try (var threadPool = new TestThreadPool(getTestName())) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> newActionModule(threadPool, List.of(secPlugin)));
            assertThat(
                e.getMessage(),
                Matchers.equalTo(
                    "The org.elasticsearch.action.ActionModuleTests$SecPlugin plugin tried to install a custom REST controller."
                        + " This functionality is not available to external plugins."
                )
            );
        }
    }

    private ActionModule newActionModule(@Nullable ThreadPool threadPool, List<ActionPlugin> plugins) {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        var resolver = threadPool == null
            ? TestIndexNameExpressionResolver.newInstance()
            : TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext());
        return new ActionModule(
            testEnv,
            resolver,
            settings.getClusterSettings(),
            settings.getSettingsFilter(),
            threadPool,
            plugins,
            null,
            null,
            new UsageService(),
            null,
            TelemetryProvider.NOOP,
            mock(ClusterService.class),
            null,
            List.of(),
            List.of(),
            RestExtension.allowAll(),
            new IncrementalBulkService(null, null, MeterRegistry.NOOP, null, null),
            CrossProjectModeDecider.NOOP,
            TestProjectResolvers.alwaysThrow()
        );
    }

    static class SecPlugin implements ActionPlugin, RestServerActionPlugin {
        private final boolean installInterceptor;
        private final boolean installContentTypePolicy;
        private final boolean installController;

        SecPlugin(boolean installInterceptor, boolean installContentTypePolicy, boolean installController) {
            this.installInterceptor = installInterceptor;
            this.installContentTypePolicy = installContentTypePolicy;
            this.installController = installController;
        }

        @Override
        public List<RestInterceptor> getRestHandlerInterceptors(ThreadContext threadContext) {
            if (installInterceptor) {
                return List.of(RestInterceptorChain::proceed);
            } else {
                return List.of();
            }
        }

        @Override
        public RestContentTypePolicy getRestContentTypePolicy(ThreadContext threadContext) {
            if (installContentTypePolicy) {
                return request -> true;
            } else {
                return null;
            }
        }

        @Override
        public RestController getRestController(
            List<RestInterceptor> interceptors,
            RestContentTypePolicy contentTypePolicy,
            NodeClient client,
            CircuitBreakerService circuitBreakerService,
            UsageService usageService,
            TelemetryProvider telemetryProvider
        ) {
            if (installController) {
                return new RestController(interceptors, contentTypePolicy, client, circuitBreakerService, usageService, telemetryProvider);
            } else {
                return null;
            }
        }
    }
}
