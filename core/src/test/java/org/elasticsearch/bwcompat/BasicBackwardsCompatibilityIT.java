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
package org.elasticsearch.bwcompat;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.Fields;
import org.apache.lucene.util.English;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESBackcompatTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class BasicBackwardsCompatibilityIT extends ESBackcompatTestCase {

    /**
     * Basic test using Index &amp; Realtime Get with external versioning. This test ensures routing works correctly across versions.
     */
    public void testExternalVersion() throws Exception {
        createIndex("test");
        final boolean routing = randomBoolean();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            String routingKey = routing ? randomRealisticUnicodeOfLength(10) : null;
            final long version = randomIntBetween(0, Integer.MAX_VALUE);
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setVersion(version).setVersionType(VersionType.EXTERNAL).setSource("field1", English.intToEnglish(i)).get();
            GetResponse get = client().prepareGet("test", "type1", id).setRouting(routingKey).setVersion(version).get();
            assertThat("Document with ID " + id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(version));
            final long nextVersion = version + randomIntBetween(0, Integer.MAX_VALUE);
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setVersion(nextVersion).setVersionType(VersionType.EXTERNAL).setSource("field1", English.intToEnglish(i)).get();
            get = client().prepareGet("test", "type1", id).setRouting(routingKey).setVersion(nextVersion).get();
            assertThat("Document with ID " + id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(nextVersion));
        }
    }

    /**
     * Basic test using Index &amp; Realtime Get with internal versioning. This test ensures routing works correctly across versions.
     */
    public void testInternalVersion() throws Exception {
        createIndex("test");
        final boolean routing = randomBoolean();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String routingKey = routing ? randomRealisticUnicodeOfLength(10) : null;
            String id = Integer.toString(i);
            assertEquals(id, DocWriteResponse.Result.CREATED, client().prepareIndex("test", "type1", id)
                .setRouting(routingKey).setSource("field1", English.intToEnglish(i)).get().getResult());
            GetResponse get = client().prepareGet("test", "type1", id).setRouting(routingKey).setVersion(1).get();
            assertThat("Document with ID " + id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(1L));
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setSource("field1", English.intToEnglish(i)).execute().actionGet();
            get = client().prepareGet("test", "type1", id).setRouting(routingKey).setVersion(2).get();
            assertThat("Document with ID " + id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(2L));
        }

        assertVersionCreated(compatibilityVersion(), "test");
    }

    /**
     * Very basic bw compat test with a mixed version cluster random indexing and lookup by ID via term query
     */
    public void testIndexAndSearch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(10, 20);
        List<IndexRequestBuilder> builder = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            builder.add(client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(i), "the_id", id));
        }
        indexRandom(true, builder);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
        assertVersionCreated(compatibilityVersion(), "test");
    }

    public void testRecoverFromPreviousVersion() throws ExecutionException, InterruptedException {
        if (backwardsCluster().numNewDataNodes() == 0) {
            backwardsCluster().startNewNode();
        }
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());
        int numDocs = randomIntBetween(100, 150);
        ArrayList<String> ids = new ArrayList<>();
        logger.info(" --> indexing [{}] docs", numDocs);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(i);
            ids.add(id);
            docs[i] = client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        if (randomBoolean()) {
            logger.info(" --> moving index to new nodes");
            backwardsCluster().allowOnlyNewNodes("test");
        } else {
            logger.info(" --> allow index to on all nodes");
            backwardsCluster().allowOnAllNodes("test");
        }

        logger.info(" --> indexing [{}] more docs", numDocs);
        // sometimes index while relocating
        if (randomBoolean()) {
            for (int i = 0; i < numDocs; i++) {
                String id = randomRealisticUnicodeOfLength(10) + String.valueOf(numDocs + i);
                ids.add(id);
                docs[i] = client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(numDocs + i));
            }
            indexRandom(true, docs);
            numDocs *= 2;
        }

        logger.info(" --> waiting for relocation of [{}] docs to complete", numDocs);
        ensureYellow("test");// move all shards to the new node (it waits on relocation)
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            assertSearchHits(client().prepareSearch().setSize(ids.size()).get(), ids.toArray(new String[ids.size()]));
        }
        assertVersionCreated(compatibilityVersion(), "test");
    }

    /**
     * Test that ensures that we will never recover from a newer to an older version (we are not forward compatible)
     */
    public void testNoRecoveryFromNewNodes() throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().backwardsNodePattern()).put(indexSettings())));
        if (backwardsCluster().numNewDataNodes() == 0) {
            backwardsCluster().startNewNode();
        }
        ensureYellow();
        assertAllShardsOnNodes("test", backwardsCluster().newNodePattern());
        if (randomBoolean()) {
            backwardsCluster().allowOnAllNodes("test");
        }
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", randomRealisticUnicodeOfLength(10) + String.valueOf(i)).setSource("field1", English.intToEnglish(i), "num_int", randomInt(), "num_double", randomDouble());
        }
        indexRandom(true, docs);
        backwardsCluster().allowOnAllNodes("test");
        while (ensureYellow() != ClusterHealthStatus.GREEN) {
            backwardsCluster().startNewNode();
        }
        assertAllShardsOnNodes("test", backwardsCluster().newNodePattern());
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            countResponse = client().prepareSearch().setSize(0).get();
            assertHitCount(countResponse, numDocs);
            assertSimpleSort("num_double", "num_int");
        }
        assertVersionCreated(compatibilityVersion(), "test");
    }


    public void assertSimpleSort(String... numericFields) {
        for (String field : numericFields) {
            SearchResponse searchResponse = client().prepareSearch().addSort(field, SortOrder.ASC).get();
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, greaterThan(0));
            Number previous = null;
            for (SearchHit hit : hits) {
                assertNotNull(hit.getSource().get(field));
                if (previous != null) {
                    assertThat(previous.doubleValue(), lessThanOrEqualTo(((Number) hit.getSource().get(field)).doubleValue()));
                }
                previous = (Number) hit.getSource().get(field);
            }
        }
    }

    @Override
    public void assertAllShardsOnNodes(String index, String pattern) {
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
    }

    /**
     * Upgrades a single node to the current version
     */
    public void testIndexUpgradeSingleNode() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i), "num_int", randomInt(), "num_double", randomDouble());
        }

        indexRandom(true, docs);
        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());
        disableAllocation("test");
        backwardsCluster().allowOnAllNodes("test");
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);
        backwardsCluster().upgradeOneNode();
        ensureYellow();
        if (randomBoolean()) {
            for (int i = 0; i < numDocs; i++) {
                docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i), "num_int", randomInt(), "num_double", randomDouble());
            }
            indexRandom(true, docs);
        }
        enableAllocation("test");
        ensureYellow();
        final int numIters = randomIntBetween(1, 20);
        for (int i = 0; i < numIters; i++) {
            assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);
            assertSimpleSort("num_double", "num_int");
        }
        assertVersionCreated(compatibilityVersion(), "test");
    }

    /**
     * Test that allocates an index on one or more old nodes and then do a rolling upgrade
     * one node after another is shut down and restarted from a newer version and we verify
     * that all documents are still around after each nodes upgrade.
     */
    public void testIndexRollingUpgrade() throws Exception {
        String[] indices = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = "test" + i;
            assertAcked(prepareCreate(indices[i]).setSettings(Settings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        }

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        String[] indexForDoc = new String[docs.length];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(indexForDoc[i] = RandomPicks.randomFrom(random(), indices), "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i), "num_int", randomInt(), "num_double", randomDouble());
        }
        indexRandom(true, docs);
        for (String index : indices) {
            assertAllShardsOnNodes(index, backwardsCluster().backwardsNodePattern());
        }
        disableAllocation(indices);
        backwardsCluster().allowOnAllNodes(indices);
        logClusterState();
        boolean upgraded;
        do {
            logClusterState();
            SearchResponse countResponse = client().prepareSearch().setSize(0).get();
            assertHitCount(countResponse, numDocs);
            assertSimpleSort("num_double", "num_int");
            upgraded = backwardsCluster().upgradeOneNode();
            ensureYellow();
            countResponse = client().prepareSearch().setSize(0).get();
            assertHitCount(countResponse, numDocs);
            for (int i = 0; i < numDocs; i++) {
                docs[i] = client().prepareIndex(indexForDoc[i], "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i), "num_int", randomInt(), "num_double", randomDouble());
            }
            indexRandom(true, docs);
        } while (upgraded);
        enableAllocation(indices);
        ensureYellow();
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);
        assertSimpleSort("num_double", "num_int");

        String[] newIndices = new String[randomIntBetween(1, 3)];

        for (int i = 0; i < newIndices.length; i++) {
            newIndices[i] = "new_index" + i;
            createIndex(newIndices[i]);
        }
        assertVersionCreated(Version.CURRENT, newIndices); // new indices are all created with the new version
        assertVersionCreated(compatibilityVersion(), indices);
    }

    public void assertVersionCreated(Version version, String... indices) {
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indices).get();
        ImmutableOpenMap<String, Settings> indexToSettings = getSettingsResponse.getIndexToSettings();
        for (String index : indices) {
            Settings settings = indexToSettings.get(index);
            assertThat(settings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null), notNullValue());
            assertThat(settings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null), equalTo(version));
        }
    }


    public void testUnsupportedFeatures() throws IOException {
        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
                .startObject()
                    .startObject("type")
                        .startObject(FieldNamesFieldMapper.NAME)
                           // by setting randomly index to no we also test the pre-1.3 behavior
                            .field("index", randomFrom("no", "not_analyzed"))
                            .field("store", randomFrom("no", "yes"))
                        .endObject()
                    .endObject()
                .endObject();

        try {
            assertAcked(prepareCreate("test").
                    setSettings(Settings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings()))
                    .addMapping("type", mapping));
        } catch (MapperParsingException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(ExceptionsHelper.detailedMessage(ex).contains("type=_field_names is not supported on indices created before version 1.3.0"), equalTo(true));
        }

    }

    /**
     * This filter had a major upgrade in 1.3 where we started to index the field names. Lets see if they still work as expected...
     * this test is basically copied from SimpleQueryTests...
     */
    public void testExistsFilter() throws IOException, ExecutionException, InterruptedException {
        int indexId = 0;
        String indexName;

        for (; ; ) {
            indexName = "test_"+indexId++;
            createIndex(indexName);
            indexRandom(true,
                    client().prepareIndex(indexName, "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()),
                    client().prepareIndex(indexName, "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()),
                    client().prepareIndex(indexName, "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()),
                    client().prepareIndex(indexName, "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()));

            SearchResponse countResponse = client().prepareSearch().setSize(0).setQuery(existsQuery("field1")).get();
            assertHitCount(countResponse, 2L);

            countResponse = client().prepareSearch().setSize(0).setQuery(constantScoreQuery(existsQuery("field1"))).get();
            assertHitCount(countResponse, 2L);

            countResponse = client().prepareSearch().setSize(0).setQuery(queryStringQuery("_exists_:field1")).get();
            assertHitCount(countResponse, 2L);

            countResponse = client().prepareSearch().setSize(0).setQuery(existsQuery("field2")).get();
            assertHitCount(countResponse, 2L);

            countResponse = client().prepareSearch().setSize(0).setQuery(existsQuery("field3")).get();
            assertHitCount(countResponse, 1L);

            // wildcard check
            countResponse = client().prepareSearch().setSize(0).setQuery(existsQuery("x*")).get();
            assertHitCount(countResponse, 2L);

            // object check
            countResponse = client().prepareSearch().setSize(0).setQuery(existsQuery("obj1")).get();
            assertHitCount(countResponse, 2L);

            countResponse = client().prepareSearch().setSize(0).setQuery(queryStringQuery("_missing_:field1")).get();
            assertHitCount(countResponse, 2L);

            if (!backwardsCluster().upgradeOneNode()) {
                break;
            }
            ensureYellow();
            assertVersionCreated(compatibilityVersion(), indexName); // we had an old node in the cluster so we have to be on the compat version
            assertAcked(client().admin().indices().prepareDelete(indexName));
        }

        assertVersionCreated(Version.CURRENT, indexName); // after upgrade we have current version
    }


    public Version getMasterVersion() {
        return client().admin().cluster().prepareState().get().getState().nodes().getMasterNode().getVersion();
    }

    public void testDeleteRoutingRequired() throws ExecutionException, InterruptedException, IOException {
        createIndexWithAlias();
        assertAcked(client().admin().indices().preparePutMapping("test").setType("test").setSource(
                XContentFactory.jsonBuilder().startObject().startObject("test").startObject("_routing").field("required", true).endObject().endObject().endObject()));
        ensureYellow("test");

        int numDocs = iterations(10, 50);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs - 2; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "test", Integer.toString(i))
                    .setRouting(randomAsciiOfLength(randomIntBetween(1, 10))).setSource("field", "value");
        }
        String firstDocId = Integer.toString(numDocs - 2);
        indexRequestBuilders[numDocs - 2] = client().prepareIndex("test", "test", firstDocId)
                .setRouting("routing").setSource("field", "value");
        String secondDocId = Integer.toString(numDocs - 1);
        String secondRouting = randomAsciiOfLength(randomIntBetween(1, 10));
        indexRequestBuilders[numDocs - 1] = client().prepareIndex("test", "test", secondDocId)
                .setRouting(secondRouting).setSource("field", "value");

        indexRandom(true, indexRequestBuilders);

        SearchResponse searchResponse = client().prepareSearch("test").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) numDocs));

        DeleteResponse deleteResponse = client().prepareDelete("test", "test", firstDocId).setRouting("routing").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        GetResponse getResponse = client().prepareGet("test", "test", firstDocId).setRouting("routing").get();
        assertThat(getResponse.isExists(), equalTo(false));
        refresh();
        searchResponse = client().prepareSearch("test").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) numDocs - 1));
    }

    public void testIndexGetAndDelete() throws ExecutionException, InterruptedException {
        createIndexWithAlias();

        int numDocs = iterations(10, 50);
        for (int i = 0; i < numDocs; i++) {
            IndexResponse indexResponse = client().prepareIndex(indexOrAlias(), "type", Integer.toString(i)).setSource("field", "value-" + i).get();
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
            assertThat(indexResponse.getIndex(), equalTo("test"));
            assertThat(indexResponse.getType(), equalTo("type"));
            assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
        }
        refresh();

        String docId = Integer.toString(randomIntBetween(0, numDocs - 1));
        GetResponse getResponse = client().prepareGet(indexOrAlias(), "type", docId).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getIndex(), equalTo("test"));
        assertThat(getResponse.getType(), equalTo("type"));
        assertThat(getResponse.getId(), equalTo(docId));

        DeleteResponse deleteResponse = client().prepareDelete(indexOrAlias(), "type", docId).get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getIndex(), equalTo("test"));
        assertThat(deleteResponse.getType(), equalTo("type"));
        assertThat(deleteResponse.getId(), equalTo(docId));

        getResponse = client().prepareGet(indexOrAlias(), "type", docId).get();
        assertThat(getResponse.isExists(), equalTo(false));

        refresh();

        SearchResponse searchResponse = client().prepareSearch(indexOrAlias()).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) numDocs - 1));
    }

    public void testUpdate() {
        createIndexWithAlias();

        UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert("field1", "value1").setDoc("field2", "value2");

        UpdateResponse updateResponse = updateRequestBuilder.get();
        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getType(), equalTo("type1"));
        assertThat(updateResponse.getId(), equalTo("1"));
        assertEquals(DocWriteResponse.Result.CREATED, updateResponse.getResult());

        GetResponse getResponse = client().prepareGet("test", "type1", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsMap().containsKey("field1"), equalTo(true));
        assertThat(getResponse.getSourceAsMap().containsKey("field2"), equalTo(false));

        updateResponse = updateRequestBuilder.get();
        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getType(), equalTo("type1"));
        assertThat(updateResponse.getId(), equalTo("1"));
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

        getResponse = client().prepareGet("test", "type1", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsMap().containsKey("field1"), equalTo(true));
        assertThat(getResponse.getSourceAsMap().containsKey("field2"), equalTo(true));
    }

    public void testAnalyze() {
        createIndexWithAlias();
        assertAcked(client().admin().indices().preparePutMapping("test").setType("test").setSource("field", "type=text,analyzer=keyword"));
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("this is a test").setIndex(indexOrAlias()).setField("field").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("this is a test"));
    }

    public void testExplain() {
        createIndexWithAlias();

        client().prepareIndex(indexOrAlias(), "test", "1").setSource("field", "value1").get();
        refresh();

        ExplainResponse response = client().prepareExplain(indexOrAlias(), "test", "1")
                .setQuery(QueryBuilders.termQuery("field", "value1")).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.isMatch(), equalTo(true));
        assertThat(response.getExplanation(), notNullValue());
        assertThat(response.getExplanation().isMatch(), equalTo(true));
        assertThat(response.getExplanation().getDetails().length, equalTo(1));
    }

    public void testGetTermVector() throws IOException {
        createIndexWithAlias();
        assertAcked(client().admin().indices().preparePutMapping("test").setType("type1").setSource("field", "type=text,term_vector=with_positions_offsets_payloads").get());

        client().prepareIndex(indexOrAlias(), "type1", "1")
                .setSource("field", "the quick brown fox jumps over the lazy dog").get();
        refresh();

        TermVectorsResponse termVectorsResponse = client().prepareTermVectors(indexOrAlias(), "type1", "1").get();
        assertThat(termVectorsResponse.getIndex(), equalTo("test"));
        assertThat(termVectorsResponse.isExists(), equalTo(true));
        Fields fields = termVectorsResponse.getFields();
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.terms("field").size(), equalTo(8L));
    }

    public void testIndicesStats() {
        createIndex("test");

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().all().get();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(1));
        assertThat(indicesStatsResponse.getIndices().containsKey("test"), equalTo(true));
    }

    public void testMultiGet() throws ExecutionException, InterruptedException {
        createIndexWithAlias();

        int numDocs = iterations(10, 50);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + Integer.toString(i));
        }
        indexRandom(false, indexRequestBuilders);

        int iterations = iterations(1, numDocs);
        MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet();
        for (int i = 0; i < iterations; i++) {
            multiGetRequestBuilder.add(new MultiGetRequest.Item(indexOrAlias(), "type", Integer.toString(randomInt(numDocs - 1))));
        }
        MultiGetResponse multiGetResponse = multiGetRequestBuilder.get();
        assertThat(multiGetResponse.getResponses().length, equalTo(iterations));
        for (int i = 0; i < multiGetResponse.getResponses().length; i++) {
            MultiGetItemResponse multiGetItemResponse = multiGetResponse.getResponses()[i];
            assertThat(multiGetItemResponse.isFailed(), equalTo(false));
            assertThat(multiGetItemResponse.getIndex(), equalTo("test"));
            assertThat(multiGetItemResponse.getType(), equalTo("type"));
            assertThat(multiGetItemResponse.getId(), equalTo(multiGetRequestBuilder.request().getItems().get(i).id()));
            assertThat(multiGetItemResponse.getResponse().isExists(), equalTo(true));
            assertThat(multiGetItemResponse.getResponse().getIndex(), equalTo("test"));
            assertThat(multiGetItemResponse.getResponse().getType(), equalTo("type"));
            assertThat(multiGetItemResponse.getResponse().getId(), equalTo(multiGetRequestBuilder.request().getItems().get(i).id()));
        }

    }

    public void testScroll() throws ExecutionException, InterruptedException {
        createIndex("test");

        int numDocs = iterations(10, 100);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + Integer.toString(i));
        }
        indexRandom(true, indexRequestBuilders);

        int size = randomIntBetween(1, 10);
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test").setScroll("1m").setSize(size);

        SearchResponse searchResponse = searchRequestBuilder.get();
        assertThat(searchResponse.getScrollId(), notNullValue());
        assertHitCount(searchResponse, numDocs);
        int hits = 0;
        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        hits += searchResponse.getHits().getHits().length;

        try {
            do {
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll("1m").get();
                assertThat(searchResponse.getScrollId(), notNullValue());
                assertHitCount(searchResponse, numDocs);
                hits += searchResponse.getHits().getHits().length;
            } while (searchResponse.getHits().getHits().length > 0);
            assertThat(hits, equalTo(numDocs));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    private void createIndexWithAlias() {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
    }
}
