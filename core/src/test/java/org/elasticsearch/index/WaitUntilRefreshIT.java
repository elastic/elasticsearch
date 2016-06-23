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

package org.elasticsearch.index;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

/**
 * Tests that requests with RefreshPolicy.WAIT_UNTIL will be visible when they return.
 */
public class WaitUntilRefreshIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(NetworkModule.HTTP_ENABLED.getKey(), true).build();
    }

    @Override
    public Settings indexSettings() {
        // Use a shorter refresh interval to speed up the tests. We'll be waiting on this interval several times.
        return Settings.builder().put(super.indexSettings()).put("index.refresh_interval", "40ms").build();
    }

    @Before
    public void createTestIndex() {
        createIndex("test");
    }

    public void testIndex() {
        IndexResponse index = client().prepareIndex("test", "index", "1").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .get();
        assertEquals(RestStatus.CREATED, index.status());
        assertFalse("request shouldn't have forced a refresh", index.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");
    }

    public void testDelete() throws InterruptedException, ExecutionException {
        // Index normally
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Now delete with blockUntilRefresh
        DeleteResponse delete = client().prepareDelete("test", "test", "1").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertTrue("document was deleted", delete.isFound());
        assertFalse("request shouldn't have forced a refresh", delete.forcedRefresh());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());
    }

    public void testUpdate() throws InterruptedException, ExecutionException {
        // Index normally
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Update with RefreshPolicy.WAIT_UNTIL
        UpdateResponse update = client().prepareUpdate("test", "test", "1").setDoc("foo", "baz").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .get();
        assertEquals(2, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "baz")).get(), "1");

        // Upsert with RefreshPolicy.WAIT_UNTIL
        update = client().prepareUpdate("test", "test", "2").setDocAsUpsert(true).setDoc("foo", "cat")
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(1, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "cat")).get(), "2");

        // Update-becomes-delete with RefreshPolicy.WAIT_UNTIL
        update = client().prepareUpdate("test", "test", "2").setScript(new Script("delete_plz", ScriptType.INLINE, "native", emptyMap()))
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(2, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "cat")).get());
    }

    public void testBulk() {
        // Index by bulk with RefreshPolicy.WAIT_UNTIL
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Update by bulk with RefreshPolicy.WAIT_UNTIL
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareUpdate("test", "test", "1").setDoc("foo", "baz"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "baz")).get(), "1");

        // Delete by bulk with RefreshPolicy.WAIT_UNTIL
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareDelete("test", "test", "1"));
        assertBulkSuccess(bulk.get());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());

        // Update makes a noop
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareDelete("test", "test", "1"));
        assertBulkSuccess(bulk.get());
    }

    /**
     * Tests that an explicit request makes block_until_refresh return. It doesn't check that block_until_refresh doesn't return until the
     * explicit refresh if the interval is -1 because we don't have that kind of control over refresh. It can happen all on its own.
     */
    public void testNoRefreshInterval() throws InterruptedException, ExecutionException {
        client().admin().indices().prepareUpdateSettings("test").setSettings(singletonMap("index.refresh_interval", -1)).get();
        ListenableActionFuture<IndexResponse> index = client().prepareIndex("test", "index", "1").setSource("foo", "bar")
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute();
        while (false == index.isDone()) {
            client().admin().indices().prepareRefresh("test").get();
        }
        assertEquals(RestStatus.CREATED, index.get().status());
        assertFalse("request shouldn't have forced a refresh", index.get().forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");
    }

    private void assertBulkSuccess(BulkResponse response) {
        assertNoFailures(response);
        for (BulkItemResponse item : response) {
            assertFalse("request shouldn't have forced a refresh", item.getResponse().forcedRefresh());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singleton(DeletePlzPlugin.class);
    }

    public static class DeletePlzPlugin extends Plugin implements ScriptPlugin {
        @Override
        public List<NativeScriptFactory> getNativeScripts() {
            return Collections.singletonList(new DeletePlzFactory());
        }
    }

    public static class DeletePlzFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            return new ExecutableScript() {
                private Map<String, Object> ctx;

                @Override
                @SuppressWarnings("unchecked")  // Elasicsearch convention
                public void setNextVar(String name, Object value) {
                    if (name.equals("ctx")) {
                        ctx = (Map<String, Object>) value;
                    }
                }

                @Override
                public Object run() {
                    ctx.put("op", "delete");
                    return null;
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "delete_plz";
        }
    }
}
