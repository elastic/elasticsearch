/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.ssl.SSLService;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ClusterScope(scope = SUITE, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class TransportFilterTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Collections.emptyList();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalPluginServerTransportServiceInterceptor.TestPlugin.class, MockTcpTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(MockTcpTransportPlugin.class);
    }

    public void test() throws Exception {
        String source = internalCluster().startNode();
        DiscoveryNode sourceNode = internalCluster().getInstance(ClusterService.class, source).localNode();
        TransportService sourceService = internalCluster().getInstance(TransportService.class, source);

        InternalPluginServerTransportServiceInterceptor sourceInterceptor = internalCluster().getInstance(PluginsService.class, source)
                .filterPlugins(InternalPluginServerTransportServiceInterceptor.TestPlugin.class).stream().findFirst().get().interceptor;

        String target = internalCluster().startNode();
        DiscoveryNode targetNode = internalCluster().getInstance(ClusterService.class, target).localNode();
        TransportService targetService = internalCluster().getInstance(TransportService.class, target);

        InternalPluginServerTransportServiceInterceptor targetInterceptor = internalCluster().getInstance(PluginsService.class, target)
                .filterPlugins(InternalPluginServerTransportServiceInterceptor.TestPlugin.class).stream().findFirst().get().interceptor;

        CountDownLatch latch = new CountDownLatch(2);
        targetService.registerRequestHandler("_action", Request::new, ThreadPool.Names.SAME,
                new RequestHandler(new Response("trgt_to_src"), latch));
        sourceService.sendRequest(targetNode, "_action", new Request("src_to_trgt"),
                new ResponseHandler(new Response("trgt_to_src"), latch));
        await(latch);

        latch = new CountDownLatch(2);
        sourceService.registerRequestHandler("_action", Request::new, ThreadPool.Names.SAME,
                new RequestHandler(new Response("src_to_trgt"), latch));
        targetService.sendRequest(sourceNode, "_action", new Request("trgt_to_src"),
                new ResponseHandler(new Response("src_to_trgt"), latch));
        await(latch);

        ServerTransportFilter sourceServerFilter = sourceInterceptor.transportFilter(TransportSettings.DEFAULT_PROFILE);
        ServerTransportFilter targetServerFilter = targetInterceptor.transportFilter(TransportSettings.DEFAULT_PROFILE);

        AuthenticationService sourceAuth = internalCluster().getInstance(AuthenticationService.class, source);
        AuthenticationService targetAuth = internalCluster().getInstance(AuthenticationService.class, target);

        InOrder inOrder = inOrder(sourceAuth, targetServerFilter, targetAuth, sourceServerFilter);
        inOrder.verify(sourceAuth).attachUserIfMissing(SystemUser.INSTANCE);
        inOrder.verify(targetServerFilter).inbound(eq("_action"), eq(new Request("src_to_trgt")), isA(TransportChannel.class));
        inOrder.verify(targetAuth).attachUserIfMissing(SystemUser.INSTANCE);
        inOrder.verify(sourceServerFilter).inbound(eq("_action"), eq(new Request("trgt_to_src")), isA(TransportChannel.class));
    }

    public static class InternalPlugin extends Plugin {
        @Override
        public Collection<Module> createGuiceModules() {
            return Collections.singletonList(new TestTransportFilterModule());
        }
    }

    public static class TestTransportFilterModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(AuthenticationService.class).toInstance(mock(AuthenticationService.class));
            bind(AuthorizationService.class).toInstance(mock(AuthorizationService.class));
        }
    }

    public static class Request extends TransportRequest {
        private String msg;

        public Request() {
        }

        Request(String msg) {
            this.msg = msg;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            msg = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(msg);
        }

        @Override
        public String toString() {
            return msg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (!msg.equals(request.msg)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return msg.hashCode();
        }
    }

    static class Response extends TransportResponse {

        private String msg;

        Response() {
        }

        Response(String msg) {
            this.msg = msg;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            msg = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(msg);
        }

        @Override
        public String toString() {
            return msg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (!msg.equals(response.msg)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return msg.hashCode();
        }
    }

    static class RequestHandler implements TransportRequestHandler<Request> {
        private final Response response;
        private final CountDownLatch latch;

        RequestHandler(Response response, CountDownLatch latch) {
            this.response = response;
            this.latch = latch;
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            channel.sendResponse(response);
            latch.countDown();
        }
    }

    class ResponseHandler implements TransportResponseHandler<Response> {
        private final Response response;
        private final CountDownLatch latch;

        ResponseHandler(Response response, CountDownLatch latch) {
            this.response = response;
            this.latch = latch;
        }

        @Override
        public Response newInstance() {
            return new Response();
        }

        @Override
        public void handleResponse(Response response) {
            assertThat(response, equalTo(this.response));
            latch.countDown();
        }

        @Override
        public void handleException(TransportException exp) {
            logger.error("execution of request failed", exp);
            fail("execution of request failed");
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private static void await(CountDownLatch latch) throws Exception {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for request");
        }
    }

    // Sub class the security transport to always inject a mock for testing
    public static class InternalPluginServerTransportServiceInterceptor extends SecurityServerTransportInterceptor {
        public static class TestPlugin extends Plugin {
            AuthenticationService authenticationService = mock(AuthenticationService.class);
            AuthorizationService authorizationService = mock(AuthorizationService.class);
            InternalPluginServerTransportServiceInterceptor interceptor;
            @Override
            public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                       ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                       SearchRequestParsers searchRequestParsers) {
                interceptor = new InternalPluginServerTransportServiceInterceptor(clusterService.getSettings(), threadPool,
                        authenticationService, authorizationService);
                return Collections.emptyList();
            }

            @Override
            public Collection<Module> createGuiceModules() {
                return Collections.singleton(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        binder.bind(AuthenticationService.class).toInstance(authenticationService);
                        binder.bind(AuthorizationService.class).toInstance(authorizationService);
                    }
                });
            }

            public void onModule(NetworkModule module) {
                module.addTransportInterceptor(new TransportInterceptor() {
                    @Override
                    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action,
                                                                                                TransportRequestHandler<T> actualHandler) {
                        return interceptor.interceptHandler(action, actualHandler);
                    }

                    @Override
                    public AsyncSender interceptSender(AsyncSender sender) {
                        return interceptor.interceptSender(sender);
                    }
                });
            }
        }

        public InternalPluginServerTransportServiceInterceptor(Settings settings, ThreadPool threadPool,
                                                               AuthenticationService authenticationService,
                                                               AuthorizationService authorizationService) {
            super(settings, threadPool,authenticationService, authorizationService, mock(XPackLicenseState.class),
                    mock(SSLService.class));
            when(licenseState.isAuthAllowed()).thenReturn(true);
        }

        @Override
        protected Map<String, ServerTransportFilter> initializeProfileFilters() {
            return Collections.singletonMap(TransportSettings.DEFAULT_PROFILE,
                    mock(ServerTransportFilter.NodeProfile.class));
        }
    }
}
