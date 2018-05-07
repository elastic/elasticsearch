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

package org.elasticsearch.indices.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.freeze.FreezeIndexResponse;
import org.elasticsearch.action.admin.indices.unfreeze.UnfreezeIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class FreezeUnfreezeIndexIT extends ESIntegTestCase {
    public void testSimpleFreezeUnfreeze() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1");
    }

    public void testSimpleFreezeMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareFreeze("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testSimpleUnfreezeMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareUnfreeze("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testFreezeOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareFreeze("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testFreezeOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");
    }

    public void testUnfreezeOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareUnfreeze("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testUnfreezeOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1");
    }

    public void testFreezeUnfreezeMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        FreezeIndexResponse freezeIndexResponse1 = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse1.isAcknowledged(), equalTo(true));
        FreezeIndexResponse freezeIndexResponse2 = client.admin().indices().prepareFreeze("test2").execute().actionGet();
        assertThat(freezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");
        assertIndexIsUnfreezeed("test3");

        UnfreezeIndexResponse unfreezeIndexResponse1 = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse1.isShardsAcknowledged(), equalTo(true));
        UnfreezeIndexResponse unfreezeIndexResponse2 = client.admin().indices().prepareUnfreeze("test2").execute().actionGet();
        assertThat(unfreezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2", "test3");
    }

    public void testFreezeUnfreezeWildcard() {
        Client client = client();
        createIndex("test1", "test2", "a");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test*").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");
        assertIndexIsUnfreezeed("a");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test*").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2", "a");
    }

    public void testFreezeUnfreezeAll() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("_all").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2", "test3");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("_all").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2", "test3");
    }

    public void testFreezeUnfreezeAllWildcard() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("*").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2", "test3");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("*").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2", "test3");
    }

    public void testFreezeNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareFreeze().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testFreezeNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareFreeze((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareUnfreeze().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareUnfreeze((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeAlreadyUnfreezeedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        //no problem if we try to unfreeze an index that's already in unfreezeed state
        UnfreezeIndexResponse unfreezeIndexResponse1 = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse1.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1");
    }

    public void testFreezeAlreadyFrozenIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        //freezing the index
        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        //no problem if we try to freeze an index that's already in frozen state
        freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");
    }

    public void testSimpleFreezeUnfreezeAlias() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        IndicesAliasesResponse aliasesResponse = client.admin().indices()
            .prepareAliases().addAlias("test1", "test1-alias").execute().actionGet();
        assertThat(aliasesResponse.isAcknowledged(), equalTo(true));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1-alias").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1-alias").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1");
    }

    public void testFreezeUnfreezeAliasMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        IndicesAliasesResponse aliasesResponse1 = client.admin().indices()
            .prepareAliases().addAlias("test1", "test-alias").execute().actionGet();
        assertThat(aliasesResponse1.isAcknowledged(), equalTo(true));
        IndicesAliasesResponse aliasesResponse2 = client.admin().indices()
            .prepareAliases().addAlias("test2", "test-alias").execute().actionGet();
        assertThat(aliasesResponse2.isAcknowledged(), equalTo(true));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test-alias").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test-alias").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2");
    }

    public void testUnfreezeWaitingForActiveShardsFailed() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                .build();
        assertAcked(client.admin().indices().prepareCreate("test").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareFreeze("test").get());

        UnfreezeIndexResponse response = client.admin().indices().prepareUnfreeze("test").setTimeout("1ms").setWaitForActiveShards(2).get();
        assertThat(response.isShardsAcknowledged(), equalTo(false));
        assertBusy(() -> assertThat(client.admin().cluster().prepareState().get().getState().metaData().index("test").getState(),
                        equalTo(IndexMetaData.State.OPEN)));
        ensureGreen("test");
    }

    private void assertIndexIsUnfreezeed(String... indices) {
        checkIndexState(IndexMetaData.State.OPEN, indices);
    }

    private void assertIndexIsFrozen(String... indices) {
        checkIndexState(IndexMetaData.State.FROZEN, indices);
    }

    private void checkIndexState(IndexMetaData.State expectedState, String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(expectedState));
        }
    }

    public void testFreezeUnfreezeWithDocs() throws IOException, ExecutionException, InterruptedException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "keyword")
                .endObject().
                endObject().
                endObject()
                .endObject());

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", mapping, XContentType.JSON));
        ensureGreen();
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").setForce(true).execute().get();
        }
        client().admin().indices().prepareFreeze("test").execute().get();

        // check the index still contains the records that we indexed
        client().admin().indices().prepareUnfreeze("test").execute().get();
        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch().setTypes("type")
            .setQuery(QueryBuilders.matchQuery("test", "init")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, docs);
    }

    public void testFreezeUnfreezeIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs ; i++) {
            client().prepareIndex("test", "type", "" + i).setSource("test", "init").execute().actionGet();
        }

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);

                // Freezing an index is not blocked
                FreezeIndexResponse freezeIndexResponse = client().admin().indices().prepareFreeze("test").execute().actionGet();
                assertAcked(freezeIndexResponse);
                assertIndexIsFrozen("test");

                // Unfreezing an index is not blocked
                UnfreezeIndexResponse unfreezeIndexResponse = client().admin().indices().prepareUnfreeze("test").execute().actionGet();
                assertAcked(unfreezeIndexResponse);
                assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
                assertIndexIsUnfreezeed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Freezing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareFreeze("test"));
                assertIndexIsUnfreezeed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        FreezeIndexResponse freezeIndexResponse = client().admin().indices().prepareFreeze("test").execute().actionGet();
        assertAcked(freezeIndexResponse);
        assertIndexIsFrozen("test");

        // Unfreezing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareUnfreeze("test"));
                assertIndexIsFrozen("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testFreezeUsesFrozenThreadPool() throws Exception {
        Client client = client();
        createIndex("test1", "test2", "test3");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            builders.add(client.prepareIndex(randomFrom("test1", "test2", "test3"), "_doc", "" + i).
                setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(true, builders);

        SearchResponse resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 7)).get();
        assertSearchHits(resp, "7");

        long nonFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                    .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                    .mapToLong(ThreadPoolStats.Stats::getCompleted)
                    .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);

        FreezeIndexResponse freezeIndexResponse1 = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse1.isAcknowledged(), equalTo(true));
        FreezeIndexResponse freezeIndexResponse2 = client.admin().indices().prepareFreeze("test2").execute().actionGet();
        assertThat(freezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");
        assertIndexIsUnfreezeed("test3");

        // Yay functional programming
        long preFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                .mapToLong(ThreadPoolStats.Stats::getCompleted)
                .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);

        assertThat(nonFrozenCompletedCounts, equalTo(preFrozenCompletedCounts));

        resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 7)).get();
        assertSearchHits(resp, "7");

        long postFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                    .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                    .mapToLong(ThreadPoolStats.Stats::getCompleted)
                    .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);
        logger.info("--> pre-search completed: {} post-search completed: {}", preFrozenCompletedCounts, postFrozenCompletedCounts);

        assertThat("expected post frozen threadpool completed count to be higher",
            postFrozenCompletedCounts, greaterThan(preFrozenCompletedCounts));

        UnfreezeIndexResponse unfreezeIndexResponse1 = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse1.isShardsAcknowledged(), equalTo(true));
        UnfreezeIndexResponse unfreezeIndexResponse2 = client.admin().indices().prepareUnfreeze("test2").execute().actionGet();
        assertThat(unfreezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfreezeed("test1", "test2", "test3");

        resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 7)).get();
        assertSearchHits(resp, "7");
    }
}
