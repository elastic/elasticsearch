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

import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.VersionTests;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.OldIndexUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.test.OldIndexUtils.assertUpgradeWorks;
import static org.elasticsearch.test.OldIndexUtils.getIndexDir;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// needs at least 2 nodes since it bumps replicas to 1
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class OldIndexBackwardsCompatibilityIT extends ESIntegTestCase {
    // TODO: test for proper exception on unsupported indexes (maybe via separate test?)
    // We have a 0.20.6.zip etc for this.


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    List<String> indexes;
    List<String> unsupportedIndexes;
    static String singleDataPathNodeName;
    static String multiDataPathNodeName;
    static Path singleDataPath;
    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = OldIndexUtils.loadDataFilesList("index", getBwcIndicesPath());
        unsupportedIndexes = OldIndexUtils.loadDataFilesList("unsupported", getBwcIndicesPath());
    }

    @AfterClass
    public static void tearDownStatics() {
        singleDataPathNodeName = null;
        multiDataPathNodeName = null;
        singleDataPath = null;
        multiDataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        return OldIndexUtils.getSettings();
    }

    void setupCluster() throws Exception {
        List<String> replicas = internalCluster().startNodes(1); // for replicas

        Path baseTempDir = createTempDir();
        // start single data path node
        Settings.Builder nodeSettings = Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("single-path").toAbsolutePath())
                .put(Node.NODE_MASTER_SETTING.getKey(), false); // workaround for dangling index loading issue when node is master
        singleDataPathNodeName = internalCluster().startNode(nodeSettings);

        // start multi data path node
        nodeSettings = Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("multi-path1").toAbsolutePath() + "," + baseTempDir
                        .resolve("multi-path2").toAbsolutePath())
                .put(Node.NODE_MASTER_SETTING.getKey(), false); // workaround for dangling index loading issue when node is master
        multiDataPathNodeName = internalCluster().startNode(nodeSettings);

        // find single data path dir
        Path[] nodePaths = internalCluster().getInstance(NodeEnvironment.class, singleDataPathNodeName).nodeDataPaths();
        assertEquals(1, nodePaths.length);
        singleDataPath = nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER);
        assertFalse(Files.exists(singleDataPath));
        Files.createDirectories(singleDataPath);
        logger.info("--> Single data path: {}", singleDataPath);

        // find multi data path dirs
        nodePaths = internalCluster().getInstance(NodeEnvironment.class, multiDataPathNodeName).nodeDataPaths();
        assertEquals(2, nodePaths.length);
        multiDataPath = new Path[]{nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER),
                nodePaths[1].resolve(NodeEnvironment.INDICES_FOLDER)};
        assertFalse(Files.exists(multiDataPath[0]));
        assertFalse(Files.exists(multiDataPath[1]));
        Files.createDirectories(multiDataPath[0]);
        Files.createDirectories(multiDataPath[1]);
        logger.info("--> Multi data paths: {}, {}", multiDataPath[0], multiDataPath[1]);
    }

    void upgradeIndexFolder() throws Exception {
        OldIndexUtils.upgradeIndexFolder(internalCluster(), singleDataPathNodeName);
        OldIndexUtils.upgradeIndexFolder(internalCluster(), multiDataPathNodeName);
    }

    void importIndex(String indexName) throws IOException {
        // force reloading dangling indices with a cluster state republish
        client().admin().cluster().prepareReroute().get();
        ensureGreen(indexName);
    }

    void unloadIndex(String indexName) throws Exception {
        assertAcked(client().admin().indices().prepareDelete(indexName).get());
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allReleasedVersions()) {
            if (VersionUtils.isSnapshot(v)) continue;  // snapshots are unreleased, so there is no backcompat yet
            if (v.isRelease() == false) continue; // no guarantees for prereleases
            if (v.before(Version.CURRENT.minimumIndexCompatibilityVersion())) continue; // we can only support one major version backward
            if (v.equals(Version.CURRENT)) continue; // the current version is always compatible with itself
            expectedVersions.add("index-" + v.toString() + ".zip");
        }

        for (String index : indexes) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: {}", index);
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

        Collections.shuffle(indexes, random());
        for (String index : indexes) {
            long startTime = System.currentTimeMillis();
            logger.info("--> Testing old index {}", index);
            assertOldIndexWorks(index);
            logger.info("--> Done testing {}, took {} seconds", index, (System.currentTimeMillis() - startTime) / 1000.0);
        }
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
        // we explicitly upgrade the index folders as these indices
        // are imported as dangling indices and not available on
        // node startup
        upgradeIndexFolder();
        importIndex(indexName);
        assertIndexSanity(indexName, version);
        assertBasicSearchWorks(indexName);
        assertAllSearchWorks(indexName);
        assertBasicAggregationWorks(indexName);
        assertRealtimeGetWorks(indexName);
        assertNewReplicasWork(indexName);
        assertUpgradeWorks(client(), indexName, version);
        assertPositionIncrementGapDefaults(indexName, version);
        assertAliasWithBadName(indexName, version);
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
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName)
            .setDetailed(true).setActiveOnly(false).get();
        boolean foundTranslog = false;
        for (List<RecoveryState> states : recoveryResponse.shardRecoveryStates().values()) {
            for (RecoveryState state : states) {
                if (state.getStage() == RecoveryState.Stage.DONE
                    && state.getPrimary()
                    && state.getRecoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    assertFalse("more than one primary recoverd?", foundTranslog);
                    assertNotEquals(0, state.getTranslog().recoveredOperations());
                    foundTranslog = true;
                }
            }
        }
        assertTrue("expected translog but nothing was recovered", foundTranslog);
        IndicesSegmentResponse segmentsResponse = client().admin().indices().prepareSegments(indexName).get();
        IndexSegments segments = segmentsResponse.getIndices().get(indexName);
        int numCurrent = 0;
        int numBWC = 0;
        for (IndexShardSegments indexShardSegments : segments) {
            for (ShardSegments shardSegments : indexShardSegments) {
                for (Segment segment : shardSegments) {
                    if (indexCreated.luceneVersion.equals(segment.version)) {
                        numBWC++;
                        if (Version.CURRENT.luceneVersion.equals(segment.version)) {
                            numCurrent++;
                        }
                    } else if (Version.CURRENT.luceneVersion.equals(segment.version)) {
                        numCurrent++;
                    } else {
                        fail("unexpected version " + segment.version);
                    }
                }
            }
        }
        assertNotEquals("expected at least 1 current segment after translog recovery", 0, numCurrent);
        assertNotEquals("expected at least 1 old segment", 0, numBWC);
        SearchResponse test = client().prepareSearch(indexName).get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
    }

    void assertBasicSearchWorks(String indexName) {
        logger.info("--> testing basic search");
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        long numDocs = searchRsp.getHits().getTotalHits();
        logger.info("Found {} in old index", numDocs);

        logger.info("--> testing basic search with sort");
        searchReq.addSort("long_sort", SortOrder.ASC);
        ElasticsearchAssertions.assertNoFailures(searchReq.get());

        logger.info("--> testing exists filter");
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.existsQuery("string"));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(numDocs, searchRsp.getHits().getTotalHits());
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        Version versionCreated = Version.fromId(Integer.parseInt(getSettingsResponse.getSetting(indexName, "index.version.created")));
        if (versionCreated.onOrAfter(Version.V_2_4_0)) {
            searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.existsQuery("field.with.dots"));
            searchRsp = searchReq.get();
            ElasticsearchAssertions.assertNoFailures(searchRsp);
            assertEquals(numDocs, searchRsp.getHits().getTotalHits());
        }
    }

    boolean findPayloadBoostInExplanation(Explanation expl) {
        if (expl.getDescription().startsWith("payloadBoost=") && expl.getValue() != 1f) {
            return true;
        } else {
            boolean found = false;
            for (Explanation sub : expl.getDetails()) {
                found |= findPayloadBoostInExplanation(sub);
            }
            return found;
        }
    }

    void assertAllSearchWorks(String indexName) {
        logger.info("--> testing _all search");
        SearchResponse searchRsp = client().prepareSearch(indexName).get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertThat(searchRsp.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
        SearchHit bestHit = searchRsp.getHits().getAt(0);

        // Make sure there are payloads and they are taken into account for the score
        // the 'string' field has a boost of 4 in the mappings so it should get a payload boost
        String stringValue = (String) bestHit.getSourceAsMap().get("string");
        assertNotNull(stringValue);
        Explanation explanation = client().prepareExplain(indexName, bestHit.getType(), bestHit.getId())
                .setQuery(QueryBuilders.matchQuery("_all", stringValue)).get().getExplanation();
        assertTrue("Could not find payload boost in explanation\n" + explanation, findPayloadBoostInExplanation(explanation));

        // Make sure the query can run on the whole index
        searchRsp = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("_all", stringValue)).setExplain(true).get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertThat(searchRsp.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
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
        client().prepareUpdate(indexName, "doc", docId).setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "bar").get();
        GetResponse getRsp = client().prepareGet(indexName, "doc", docId).get();
        Map<String, Object> source = getRsp.getSourceAsMap();
        assertThat(source, Matchers.hasKey("foo"));

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                .put("refresh_interval", IndexSettings.DEFAULT_REFRESH_INTERVAL)
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

    void assertPositionIncrementGapDefaults(String indexName, Version version) throws Exception {
        client().prepareIndex(indexName, "doc", "position_gap_test").setSource("string", Arrays.asList("one", "two three"))
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        // Baseline - phrase query finds matches in the same field value
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "two three")).get(), 1);

        // No match across gaps when slop < position gap
        assertHitCount(
                client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(99)).get(),
                0);

        // Match across gaps when slop >= position gap
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(100)).get(), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(101)).get(),
                1);

        // No match across gap using default slop with default positionIncrementGap
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two")).get(), 0);

        // Nor with small-ish values
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(5)).get(), 0);
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(50)).get(), 0);

        // But huge-ish values still match
        assertHitCount(client().prepareSearch(indexName).setQuery(matchPhraseQuery("string", "one two").slop(500)).get(), 1);
    }

    private static final Version VERSION_5_1_0_UNRELEASED = Version.fromString("5.1.0");

    public void testUnreleasedVersion() {
        VersionTests.assertUnknownVersion(VERSION_5_1_0_UNRELEASED);
    }

    /**
     * Search on an alias that contains illegal characters that would prevent it from being created after 5.1.0. It should still be
     * search-able though.
     */
    void assertAliasWithBadName(String indexName, Version version) throws Exception {
        if (version.onOrAfter(VERSION_5_1_0_UNRELEASED)) {
            return;
        }
        // We can read from the alias just like we can read from the index.
        String aliasName = "#" + indexName;
        long totalDocs = client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits();
        assertHitCount(client().prepareSearch(aliasName).setSize(0).get(), totalDocs);
        assertThat(totalDocs, greaterThanOrEqualTo(2000L));

        // We can remove the alias.
        assertAcked(client().admin().indices().prepareAliases().removeAlias(indexName, aliasName).get());
        assertFalse(client().admin().indices().prepareAliasesExist(aliasName).get().exists());
    }

    /**
     * Make sure we can load stored binary fields.
     */
    void assertStoredBinaryFields(String indexName, Version version) throws Exception {
        SearchRequestBuilder builder = client().prepareSearch(indexName);
        builder.setQuery(QueryBuilders.matchAllQuery());
        builder.setSize(100);
        builder.addStoredField("binary");
        SearchHits hits = builder.get().getHits();
        assertEquals(100, hits.getHits().length);
        for(SearchHit hit : hits) {
            SearchHitField field = hit.field("binary");
            assertNotNull(field);
            Object value = field.getValue();
            assertTrue(value instanceof BytesArray);
            assertEquals(16, ((BytesArray) value).length());
        }
    }

    private Path getNodeDir(String indexFile) throws IOException {
        Path unzipDir = createTempDir();
        Path unzipDataDir = unzipDir.resolve("data");

        // decompress the index
        Path backwardsIndex = getBwcIndicesPath().resolve(indexFile);
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, unzipDir);
        }

        // check it is unique
        assertTrue(Files.exists(unzipDataDir));
        Path[] list = FileSystemUtils.files(unzipDataDir);
        if (list.length != 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }

        int zipIndex = indexFile.indexOf(".zip");
        final Version version = Version.fromString(indexFile.substring("index-".length(), zipIndex));
        if (version.before(Version.V_5_0_0_alpha1)) {
            // the bwc scripts packs the indices under this path
            return list[0].resolve("nodes/0/");
        } else {
            // after 5.0.0, data folders do not include the cluster name
            return list[0].resolve("0");
        }
    }

    public void testOldClusterStates() throws Exception {
        // dangling indices do not load the global state, only the per-index states
        // so we make sure we can read them separately
        MetaDataStateFormat<MetaData> globalFormat = new MetaDataStateFormat<MetaData>(XContentType.JSON, "global-") {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
        MetaDataStateFormat<IndexMetaData> indexFormat = new MetaDataStateFormat<IndexMetaData>(XContentType.JSON, "state-") {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
        Collections.shuffle(indexes, random());
        for (String indexFile : indexes) {
            String indexName = indexFile.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
            Path nodeDir = getNodeDir(indexFile);
            logger.info("Parsing cluster state files from index [{}]", indexName);
            final MetaData metaData = globalFormat.loadLatestState(logger, xContentRegistry(), nodeDir);
            assertNotNull(metaData);

            final Version version = Version.fromString(indexName.substring("index-".length()));
            final Path dataDir;
            if (version.before(Version.V_5_0_0_alpha1)) {
                dataDir = nodeDir.getParent().getParent();
            } else {
                dataDir = nodeDir.getParent();
            }
            final Path indexDir = getIndexDir(logger, indexName, indexFile, dataDir);
            assertNotNull(indexFormat.loadLatestState(logger, xContentRegistry(), indexDir));
        }
    }

}
