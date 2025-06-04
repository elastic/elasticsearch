/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.containsString;

public class ReindexFromRemoteWithAuthTests extends ESSingleNodeTestCase {
    private TransportAddress address;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            Netty4Plugin.class,
            ReindexFromRemoteWithAuthTests.TestPlugin.class,
            ReindexPlugin.class,
            MainRestPlugin.class
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        // Whitelist reindexing from the http host we're going to use
        settings.put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:*");
        settings.put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME);
        return settings.build();
    }

    @Before
    public void setupSourceIndex() {
        prepareIndex("source").setSource("test", "test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
    }

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = clusterAdmin().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getInfo(HttpInfo.class).getAddress().publishAddress();
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String username, String password, Map<String, String> headers) {
        return new RemoteInfo(
            "http",
            address.getAddress(),
            address.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            username,
            password == null ? null : new SecureString(password.toCharArray()),
            headers,
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
    }

    public void testReindexFromRemoteWithAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source("source")
            .destination("dest")
            .setRemoteInfo(newRemoteInfo("Aladdin", "open sesame", emptyMap()));
        assertThat(request.get(), matcher().created(1));
    }

    public void testReindexSendsHeaders() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source("source")
            .destination("dest")
            .setRemoteInfo(newRemoteInfo(null, null, singletonMap(TestFilter.EXAMPLE_HEADER, "doesn't matter")));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getMessage(), containsString("Hurray! Sent the header!"));
    }

    public void testReindexWithoutAuthenticationWhenRequired() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source("source")
            .destination("dest")
            .setRemoteInfo(newRemoteInfo(null, null, emptyMap()));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
        assertThat(e.getMessage(), containsString("\"reason\":\"Authentication required\""));
        assertThat(e.getMessage(), containsString("\"WWW-Authenticate\":\"Basic realm=auth-realm\""));
    }

    public void testReindexWithBadAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source("source")
            .destination("dest")
            .setRemoteInfo(newRemoteInfo("junk", "auth", emptyMap()));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertThat(e.getMessage(), containsString("\"reason\":\"Bad Authorization\""));
    }

    /**
     * Plugin that demands authentication.
     */
    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<ReindexFromRemoteWithAuthTests.TestFilter> testFilter = new SetOnce<>();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            testFilter.set(new ReindexFromRemoteWithAuthTests.TestFilter(services.threadPool()));
            return Collections.emptyList();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(testFilter.get());
        }

        @Override
        public Collection<RestHeaderDefinition> getRestHeaders() {
            return Arrays.asList(
                new RestHeaderDefinition(TestFilter.AUTHORIZATION_HEADER, false),
                new RestHeaderDefinition(TestFilter.EXAMPLE_HEADER, false)
            );
        }
    }

    /**
     * Action filter that will reject the request if it isn't authenticated.
     */
    public static class TestFilter implements ActionFilter {
        /**
         * The authorization required. Corresponds to username="Aladdin" and password="open sesame". It is the example in
         * <a href="https://tools.ietf.org/html/rfc1945#section-11.1">HTTP/1.0's RFC</a>.
         */
        private static final String REQUIRED_AUTH = "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        private static final String AUTHORIZATION_HEADER = "Authorization";
        private static final String EXAMPLE_HEADER = "Example-Header";
        private final ThreadContext context;

        public TestFilter(ThreadPool threadPool) {
            context = threadPool.getThreadContext();
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            if (false == action.equals(TransportSearchAction.TYPE.name())) {
                chain.proceed(task, action, request, listener);
                return;
            }
            if (context.getHeader(EXAMPLE_HEADER) != null) {
                throw new IllegalArgumentException("Hurray! Sent the header!");
            }
            String auth = context.getHeader(AUTHORIZATION_HEADER);
            if (auth == null) {
                ElasticsearchSecurityException e = new ElasticsearchSecurityException("Authentication required", RestStatus.UNAUTHORIZED);
                e.addHeader("WWW-Authenticate", "Basic realm=auth-realm");
                throw e;
            }
            if (false == REQUIRED_AUTH.equals(auth)) {
                throw new ElasticsearchSecurityException("Bad Authorization", RestStatus.FORBIDDEN);
            }
            chain.proceed(task, action, request, listener);
        }
    }
}
