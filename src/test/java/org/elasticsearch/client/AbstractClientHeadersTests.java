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

package org.elasticsearch.client;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class AbstractClientHeadersTests extends ElasticsearchTestCase {

    protected static final Settings HEADER_SETTINGS = ImmutableSettings.builder()
            .put(Headers.PREFIX + ".key1", "val1")
            .put(Headers.PREFIX + ".key2", "val 2")
            .build();

    @SuppressWarnings("unchecked")
    private static final GenericAction[] ACTIONS = new GenericAction[] {
                // client actions
                GetAction.INSTANCE, SearchAction.INSTANCE, DeleteAction.INSTANCE, DeleteIndexedScriptAction.INSTANCE,
                IndexAction.INSTANCE,

                // cluster admin actions
                ClusterStatsAction.INSTANCE, CreateSnapshotAction.INSTANCE, ClusterRerouteAction.INSTANCE,

                // indices admin actions
                CreateIndexAction.INSTANCE, IndicesStatsAction.INSTANCE, ClearIndicesCacheAction.INSTANCE, FlushAction.INSTANCE
    };

    private Client client;

    @Before
    public void initClient() {
        client = buildClient(HEADER_SETTINGS, ACTIONS);
    }

    @After
    public void cleanupClient() {
        client.close();
    }

    protected abstract Client buildClient(Settings headersSettings, GenericAction[] testedActions);


    @Test
    public void testActions() {

        // TODO this is a really shitty way to test it, we need to figure out a way to test all the client methods
        //      without specifying each one (reflection doesn't as each action needs its own special settings, without
        //      them, request validation will fail before the test is executed. (one option is to enable disabling the
        //      validation in the settings??? - ugly and conceptually wrong)

        // choosing arbitrary top level actions to test
        client.prepareGet("idx", "type", "id").execute().addListener(new AssertingActionListener<GetResponse>(GetAction.NAME));
        client.prepareSearch().execute().addListener(new AssertingActionListener<SearchResponse>(SearchAction.NAME));
        client.prepareDelete("idx", "type", "id").execute().addListener(new AssertingActionListener<DeleteResponse>(DeleteAction.NAME));
        client.prepareDeleteIndexedScript("lang", "id").execute().addListener(new AssertingActionListener<DeleteIndexedScriptResponse>(DeleteIndexedScriptAction.NAME));
        client.prepareIndex("idx", "type", "id").setSource("source").execute().addListener(new AssertingActionListener<IndexResponse>(IndexAction.NAME));

        // choosing arbitrary cluster admin actions to test
        client.admin().cluster().prepareClusterStats().execute().addListener(new AssertingActionListener<ClusterStatsResponse>(ClusterStatsAction.NAME));
        client.admin().cluster().prepareCreateSnapshot("repo", "bck").execute().addListener(new AssertingActionListener<CreateSnapshotResponse>(CreateSnapshotAction.NAME));
        client.admin().cluster().prepareReroute().execute().addListener(new AssertingActionListener<ClusterRerouteResponse>(ClusterRerouteAction.NAME));

        // choosing arbitrary indices admin actions to test
        client.admin().indices().prepareCreate("idx").execute().addListener(new AssertingActionListener<CreateIndexResponse>(CreateIndexAction.NAME));
        client.admin().indices().prepareStats().execute().addListener(new AssertingActionListener<IndicesStatsResponse>(IndicesStatsAction.NAME));
        client.admin().indices().prepareClearCache("idx1", "idx2").execute().addListener(new AssertingActionListener<ClearIndicesCacheResponse>(ClearIndicesCacheAction.NAME));
        client.admin().indices().prepareFlush().execute().addListener(new AssertingActionListener<FlushResponse>(FlushAction.NAME));
    }

    @Test
    public void testOverideHeader() throws Exception {
        String key1Val = randomAsciiOfLength(5);
        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("key1", key1Val)
                .put("key2", "val 2")
                .build();

        client.prepareGet("idx", "type", "id")
                .putHeader("key1", key1Val)
                .execute().addListener(new AssertingActionListener<GetResponse>(GetAction.NAME, expected));

        client.admin().cluster().prepareClusterStats()
                .putHeader("key1", key1Val)
                .execute().addListener(new AssertingActionListener<ClusterStatsResponse>(ClusterStatsAction.NAME, expected));

        client.admin().indices().prepareCreate("idx")
                .putHeader("key1", key1Val)
                .execute().addListener(new AssertingActionListener<CreateIndexResponse>(CreateIndexAction.NAME, expected));
    }

    protected static void assertHeaders(Map<String, Object> headers, Map<String, Object> expected) {
        assertThat(headers, notNullValue());
        assertThat(headers.size(), is(expected.size()));
        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            assertThat(headers.get(expectedEntry.getKey()), equalTo(expectedEntry.getValue()));
        }
    }

    protected static void assertHeaders(TransportMessage<?> message) {
        assertHeaders(message, HEADER_SETTINGS.getAsSettings(Headers.PREFIX).getAsStructuredMap());
    }

    protected static void assertHeaders(TransportMessage<?> message, Map<String, Object> expected) {
        assertThat(message.getHeaders(), notNullValue());
        assertThat(message.getHeaders().size(), is(expected.size()));
        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            assertThat(message.getHeader(expectedEntry.getKey()), equalTo(expectedEntry.getValue()));
        }
    }

    protected static class InternalException extends Exception {

        private final String action;
        private final Map<String, Object> headers;

        public InternalException(String action, TransportMessage<?> message) {
            this.action = action;
            this.headers = new HashMap<>();
            for (String key : message.getHeaders()) {
                headers.put(key, message.getHeader(key));
            }
        }
    }

    protected static class AssertingActionListener<T> implements ActionListener<T> {

        private final String action;
        private final Map<String, Object> expectedHeaders;

        public AssertingActionListener(String action) {
            this(action, HEADER_SETTINGS.getAsSettings(Headers.PREFIX).getAsStructuredMap());
        }

       public AssertingActionListener(String action, Map<String, Object> expectedHeaders) {
            this.action = action;
            this.expectedHeaders = expectedHeaders;
        }

        @Override
        public void onResponse(T t) {
            fail("an internal exception was expected for action [" + action + "]");
        }

        @Override
        public void onFailure(Throwable t) {
            Throwable e = unwrap(t, InternalException.class);
            assertThat("expected action [" + action + "] to throw an internal exception", e, notNullValue());
            assertThat(action, equalTo(((InternalException) e).action));
            Map<String, Object> headers = ((InternalException) e).headers;
            assertHeaders(headers, expectedHeaders);
        }

        public Throwable unwrap(Throwable t, Class<? extends Throwable> exceptionType) {
            int counter = 0;
            Throwable result = t;
            while (!exceptionType.isInstance(result)) {
                if (result.getCause() == null) {
                    return null;
                }
                if (result.getCause() == result) {
                    return null;
                }
                if (counter++ > 10) {
                    // dear god, if we got more than 10 levels down, WTF? just bail
                    fail("Exception cause unwrapping ran for 10 levels: " + Throwables.getStackTraceAsString(t));
                    return null;
                }
                result = result.getCause();
            }
            return result;
        }

    }

}
