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

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.MultiDataPathUpgrader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.string.StringFieldMapperPositionIncrementGapTests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.OldIndexUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.test.OldIndexUtils.assertUpgradeWorks;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// needs at least 2 nodes since it bumps replicas to 1
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class OldIndexBackwardsCompatibilityIT extends ESIntegTestCase {
    // TODO: test for proper exception on unsupported indexes (maybe via separate test?)
    // We have a 0.20.6.zip etc for this.

    List<String> indexes;
    List<String> unsupportedIndexes;
    static Path singleDataPath;
    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = OldIndexUtils.loadIndexesList("index", getBwcIndicesPath());
        unsupportedIndexes = OldIndexUtils.loadIndexesList("unsupported", getBwcIndicesPath());
    }

    @AfterClass
    public static void tearDownStatics() {
        singleDataPath = null;
        multiDataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        return OldIndexUtils.getSettings();
    }

    void setupCluster() throws Exception {
        ListenableFuture<List<String>> replicas = internalCluster().startNodesAsync(1); // for replicas

        Path baseTempDir = createTempDir();
        // start single data path node
        Settings.Builder nodeSettings = Settings.builder()
            .put("path.data", baseTempDir.resolve("single-path").toAbsolutePath())
            .put("node.master", false); // workaround for dangling index loading issue when node is master
        ListenableFuture<String> singleDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());

        // start multi data path node
        nodeSettings = Settings.builder()
            .put("path.data", baseTempDir.resolve("multi-path1").toAbsolutePath() + "," + baseTempDir.resolve("multi-path2").toAbsolutePath())
            .put("node.master", false); // workaround for dangling index loading issue when node is master
        ListenableFuture<String> multiDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());

        // find single data path dir
        Path[] nodePaths = internalCluster().getInstance(NodeEnvironment.class, singleDataPathNode.get()).nodeDataPaths();
        assertEquals(1, nodePaths.length);
        singleDataPath = nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER);
        assertFalse(Files.exists(singleDataPath));
        Files.createDirectories(singleDataPath);
        logger.info("--> Single data path: " + singleDataPath.toString());

        // find multi data path dirs
        nodePaths = internalCluster().getInstance(NodeEnvironment.class, multiDataPathNode.get()).nodeDataPaths();
        assertEquals(2, nodePaths.length);
        multiDataPath = new Path[]{nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER),
                nodePaths[1].resolve(NodeEnvironment.INDICES_FOLDER)};
        assertFalse(Files.exists(multiDataPath[0]));
        assertFalse(Files.exists(multiDataPath[1]));
        Files.createDirectories(multiDataPath[0]);
        Files.createDirectories(multiDataPath[1]);
        logger.info("--> Multi data paths: " + multiDataPath[0].toString() + ", " + multiDataPath[1].toString());

        replicas.get(); // wait for replicas
    }

    void importIndex(String indexName) throws IOException {
        final Iterable<NodeEnvironment> instances = internalCluster().getInstances(NodeEnvironment.class);
        for (NodeEnvironment nodeEnv : instances) { // upgrade multidata path
            MultiDataPathUpgrader.upgradeMultiDataPath(nodeEnv, logger);
        }
        // force reloading dangling indices with a cluster state republish
        client().admin().cluster().prepareReroute().get();
        ensureGreen(indexName);
    }

    void unloadIndex(String indexName) throws Exception {
        assertAcked(client().admin().indices().prepareDelete(indexName).get());
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allVersions()) {
            if (v.snapshot()) continue;  // snapshots are unreleased, so there is no backcompat yet
            if (v.onOrBefore(Version.V_0_20_6)) continue; // we can only test back one major lucene version
            if (v.equals(Version.CURRENT)) continue; // the current version is always compatible with itself
            expectedVersions.add("index-" + v.toString() + ".zip");
        }

        for (String index : indexes) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: " + index);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old index tests are missing indexes:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    public void testOldIndexes() throws Exception {
        setupCluster();

        Collections.shuffle(indexes, getRandom());
        for (String index : indexes) {
            long startTime = System.currentTimeMillis();
            logger.info("--> Testing old index " + index);
            assertOldIndexWorks(index);
            logger.info("--> Done testing " + index + ", took " + ((System.currentTimeMillis() - startTime) / 1000.0) + " seconds");
        }
    }

    @Test
    public void testHandlingOfUnsupportedDanglingIndexes() throws Exception {
        setupCluster();
        Collections.shuffle(unsupportedIndexes, getRandom());
        for (String index : unsupportedIndexes) {
            assertUnsupportedIndexHandling(index);
        }
    }

    /**
     * Waits for the index to show up in the cluster state in closed state
     */
    void ensureClosed(final String index) throws InterruptedException {
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                return state.metaData().hasIndex(index) && state.metaData().index(index).getState() == IndexMetaData.State.CLOSE;
            }
        }));
    }

    /**
     * Checks that the given index cannot be opened due to incompatible version
     */
    void assertUnsupportedIndexHandling(String index) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("--> Testing old index " + index);
        Version version = OldIndexUtils.extractVersion(index);
        Path[] paths;
        if (randomBoolean()) {
            logger.info("--> injecting index [{}] into single data path", index);
            paths = new Path[]{singleDataPath};
        } else {
            logger.info("--> injecting index [{}] into multi data path", index);
            paths = multiDataPath;
        }

        String indexName = index.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
        OldIndexUtils.loadIndex(indexName, index, createTempDir(), getBwcIndicesPath(), logger, paths);
        // force reloading dangling indices with a cluster state republish
        client().admin().cluster().prepareReroute().get();
        ensureClosed(indexName);
        try {
            client().admin().indices().prepareOpen(indexName).get();
            fail("Shouldn't be able to open an old index");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("was created before v0.90.0 and wasn't upgraded"));
        }
        unloadIndex(indexName);
        logger.info("--> Done testing " + index + ", took " + ((System.currentTimeMillis() - startTime) / 1000.0) + " seconds");
    }

    void assertOldIndexWorks(String index) throws Exception {
        Version version = OldIndexUtils.extractVersion(index);
        Path[] paths;
        if (randomBoolean()) {
            logger.info("--> injecting index [{}] into single data path", index);
            paths = new Path[]{singleDataPath};
        } else {
            logger.info("--> injecting index [{}] into multi data path", index);
            paths = multiDataPath;
        }

        String indexName = index.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
        OldIndexUtils.loadIndex(indexName, index, createTempDir(), getBwcIndicesPath(), logger, paths);
        importIndex(indexName);
        assertIndexSanity(indexName, version);
        assertBasicSearchWorks(indexName);
        assertBasicAggregationWorks(indexName);
        assertRealtimeGetWorks(indexName);
        assertNewReplicasWork(indexName);
        assertUpgradeWorks(client(), indexName, version);
        assertDeleteByQueryWorked(indexName, version);
        assertPositionIncrementGapDefaults(indexName, version);
        assertStoredBinaryFields(indexName, version);
        unloadIndex(indexName);
    }

    void assertIndexSanity(String indexName, Version indexCreated) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices(indexName).get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals(indexName, getIndexResponse.indices()[0]);
        Version actualVersionCreated = Version.indexCreated(getIndexResponse.getSettings().get(indexName));
        assertEquals(indexCreated, actualVersionCreated);
        ensureYellow(indexName);
        IndicesSegmentResponse segmentsResponse = client().admin().indices().prepareSegments(indexName).get();
        IndexSegments segments = segmentsResponse.getIndices().get(indexName);
        for (IndexShardSegments indexShardSegments : segments) {
            for (ShardSegments shardSegments : indexShardSegments) {
                for (Segment segment : shardSegments) {
                    assertEquals(indexCreated.toString(), indexCreated.luceneVersion, segment.version);
                }
            }
        }
        SearchResponse test = client().prepareSearch(indexName).get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
    }

    void assertBasicSearchWorks(String indexName) {
        logger.info("--> testing basic search");
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        long numDocs = searchRsp.getHits().getTotalHits();
        logger.info("Found " + numDocs + " in old index");

        logger.info("--> testing basic search with sort");
        searchReq.addSort("long_sort", SortOrder.ASC);
        ElasticsearchAssertions.assertNoFailures(searchReq.get());

        logger.info("--> testing exists filter");
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.existsQuery("string"));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(numDocs, searchRsp.getHits().getTotalHits());

        logger.info("--> testing missing filter");
        // the field for the missing filter here needs to be different than the exists filter above, to avoid being found in the cache
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.missingQuery("long_sort"));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(0, searchRsp.getHits().getTotalHits());
    }

    void assertBasicAggregationWorks(String indexName) {
        // histogram on a long
        SearchResponse searchRsp = client().prepareSearch(indexName).addAggregation(AggregationBuilders.histogram("histo").field
                ("long_sort").interval(10)).get();
        ElasticsearchAssertions.assertSearchResponse(searchRsp);
        Histogram histo = searchRsp.getAggregations().get("histo");
        assertNotNull(histo);
        long totalCount = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            totalCount += bucket.getDocCount();
        }
        assertEquals(totalCount, searchRsp.getHits().getTotalHits());

        // terms on a boolean
        searchRsp = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("bool_terms").field("bool")).get();
        Terms terms = searchRsp.getAggregations().get("bool_terms");
        totalCount = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            totalCount += bucket.getDocCount();
        }
        assertEquals(totalCount, searchRsp.getHits().getTotalHits());
    }

    void assertRealtimeGetWorks(String indexName) {
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                .put("refresh_interval", -1)
                .build()));
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        SearchHit hit = searchReq.get().getHits().getAt(0);
        String docId = hit.getId();
        // foo is new, it is not a field in the generated index
        client().prepareUpdate(indexName, "doc", docId).setDoc("foo", "bar").get();
        GetResponse getRsp = client().prepareGet(indexName, "doc", docId).get();
        Map<String, Object> source = getRsp.getSourceAsMap();
        assertThat(source, Matchers.hasKey("foo"));

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                .put("refresh_interval", EngineConfig.DEFAULT_REFRESH_INTERVAL)
                .build()));
    }

    void assertNewReplicasWork(String indexName) throws Exception {
        final int numReplicas = 1;
        final long startTime = System.currentTimeMillis();
        logger.debug("--> creating [{}] replicas for index [{}]", numReplicas, indexName);
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                .put("number_of_replicas", numReplicas)
        ).execute().actionGet());
        ensureGreen(TimeValue.timeValueMinutes(2), indexName);
        logger.debug("--> index [{}] is green, took [{}]", indexName, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
        logger.debug("--> recovery status:\n{}", XContentHelper.toString(client().admin().indices().prepareRecoveries(indexName).get()));

        // TODO: do something with the replicas! query? index?
    }

    // #10067: create-bwc-index.py deleted any doc with long_sort:[10-20]
    void assertDeleteByQueryWorked(String indexName, Version version) throws Exception {
        if (version.onOrBefore(Version.V_1_0_0_Beta2) || version.onOrAfter(Version.V_2_0_0_beta1)) {
            // TODO: remove this once #10262 is fixed
            return;
        }
        // these documents are supposed to be deleted by a delete by query operation in the translog
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.queryStringQuery("long_sort:[10 TO 20]"));
        assertEquals(0, searchReq.get().getHits().getTotalHits());
    }

    void assertPositionIncrementGapDefaults(String indexName, Version version) throws Exception {
        if (version.before(Version.V_2_0_0_beta1)) {
            StringFieldMapperPositionIncrementGapTests.assertGapIsZero(client(), indexName, "doc");
        } else {
            StringFieldMapperPositionIncrementGapTests.assertGapIsOneHundred(client(), indexName, "doc");
        }
    }

    /**
     * Make sure we can load stored binary fields.
     */
    void assertStoredBinaryFields(String indexName, Version version) throws Exception {
        if (version.onOrAfter(Version.V_1_0_0) == false) {
            // not sure why but I can't get binary fields properly indexed w/ ES < 1.0.0
            return;
        }
        boolean hasCompressedBinaryField = version.onOrAfter(Version.V_2_0_0_beta1) == false;

        SearchRequestBuilder request = client().prepareSearch(indexName);
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setSize(100);
        request.addField("binary");
        if (hasCompressedBinaryField) {
            request.addField("binary_compressed");
        }
        SearchHits hits = request.get().getHits();
        assertEquals(100, hits.hits().length);

        for(SearchHit hit : hits) {
            SearchHitField field = hit.field("binary");
            assertNotNull("version=" + version + " has no binary field", field);
            Object value = field.value();
            assertTrue("got: " + value + " version=" + version, value instanceof BytesReference);
            assertEquals(16, ((BytesReference) value).length());

            if (hasCompressedBinaryField) {
                field = hit.field("binary_compressed");
                assertNotNull("version=" + version + " has no binary_compressed field", field);
                value = field.value();
                assertTrue("got: " + value + " version=" + version, value instanceof BytesReference);
                assertEquals(16, ((BytesReference) value).length());
            }
        }
    }
}
