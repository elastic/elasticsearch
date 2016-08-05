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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.containsString;

public class ReindexFromRemoteWithAuthTests extends ESSingleNodeTestCase {
    private TransportAddress address;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            Netty4Plugin.class,
            ReindexFromRemoteWithAuthTests.TestPlugin.class,
            ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        // Weird incantation required to test with netty
        settings.put(NetworkModule.HTTP_ENABLED.getKey(), true);
        // Whitelist reindexing from the http host we're going to use
        settings.put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "myself");
        settings.put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME);
        return settings.build();
    }

    @Before
    public void setupSourceIndex() {
        client().prepareIndex("source", "test").setSource("test", "test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
    }

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getHttp().getAddress().publishAddress();
    }

    public void testReindexFromRemoteWithAuthentication() throws Exception {
        RemoteInfo remote = new RemoteInfo("http", address.getHost(), address.getPort(), new BytesArray("{\"match_all\":{}}"), "Aladdin",
                "open sesame", emptyMap());
        ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest")
                .setRemoteInfo(remote);
        assertThat(request.get(), matcher().created(1));
    }

    public void testReindexSendsHeaders() throws Exception {
        RemoteInfo remote = new RemoteInfo("http", address.getHost(), address.getPort(), new BytesArray("{\"match_all\":{}}"), null, null,
                singletonMap(TestFilter.EXAMPLE_HEADER, "doesn't matter"));
        ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest")
                .setRemoteInfo(remote);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getMessage(), containsString("Hurray! Sent the header!"));
    }

    public void testReindexWithoutAuthenticationWhenRequired() throws Exception {
        RemoteInfo remote = new RemoteInfo("http", address.getHost(), address.getPort(), new BytesArray("{\"match_all\":{}}"), null, null,
                emptyMap());
        ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest")
                .setRemoteInfo(remote);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
        assertThat(e.getMessage(), containsString("\"reason\":\"Authentication required\""));
        assertThat(e.getMessage(), containsString("\"WWW-Authenticate\":\"Basic realm=auth-realm\""));
    }

    public void testReindexWithBadAuthentication() throws Exception {
        RemoteInfo remote = new RemoteInfo("http", address.getHost(), address.getPort(), new BytesArray("{\"match_all\":{}}"), "junk",
                "auth", emptyMap());
        ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest")
                .setRemoteInfo(remote);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> request.get());
        assertThat(e.getMessage(), containsString("\"reason\":\"Bad Authorization\""));
    }

    /**
     * Plugin that demands authentication.
     */
    public static class TestPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<Class<? extends ActionFilter>> getActionFilters() {
            return singletonList(ReindexFromRemoteWithAuthTests.TestFilter.class);
        }

        @Override
        public Collection<String> getRestHeaders() {
            return Arrays.asList(TestFilter.AUTHORIZATION_HEADER, TestFilter.EXAMPLE_HEADER);
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

        @Inject
        public TestFilter(ThreadPool threadPool) {
            context = threadPool.getThreadContext();
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public <Request extends ActionRequest<Request>, Response extends ActionResponse> void apply(Task task, String action,
                Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            if (false == action.equals(SearchAction.NAME)) {
                chain.proceed(task, action, request, listener);
                return;
            }
            if (context.getHeader(EXAMPLE_HEADER) != null) {
                throw new IllegalArgumentException("Hurray! Sent the header!");
            }
            String auth = context.getHeader(AUTHORIZATION_HEADER);
            if (auth == null) {
                ElasticsearchSecurityException e = new ElasticsearchSecurityException("Authentication required",
                        RestStatus.UNAUTHORIZED);
                e.addHeader("WWW-Authenticate", "Basic realm=auth-realm");
                throw e;
            }
            if (false == REQUIRED_AUTH.equals(auth)) {
                throw new ElasticsearchSecurityException("Bad Authorization", RestStatus.FORBIDDEN);
            }
            chain.proceed(task, action, request, listener);
        }

        @Override
        public <Response extends ActionResponse> void apply(String action, Response response, ActionListener<Response> listener,
                ActionFilterChain<?, Response> chain) {
            chain.proceed(action, response, listener);
        }
    }
}
