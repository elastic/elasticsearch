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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.upgrade.UpgradeIT;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.IndexFolderUpgrader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.string.StringFieldMapperPositionIncrementGapTests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// needs at least 2 nodes since it bumps replicas to 1
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class OldIndexBackwardsCompatibilityIT extends ESIntegTestCase {
    // TODO: test for proper exception on unsupported indexes (maybe via separate test?)
    // We have a 0.20.6.zip etc for this.


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    List<String> indexes;
    List<String> unsupportedIndexes;
    static String singleDataPathNodeName;
    static String multiDataPathNodeName;
    static Path singleDataPath;
    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = loadIndexesList("index");
        unsupportedIndexes = loadIndexesList("unsupported");
    }

    private List<String> loadIndexesList(String prefix) throws IOException {
        List<String> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getBwcIndicesPath(), prefix + "-*.zip")) {
            for (Path path : stream) {
                indexes.add(path.getFileName().toString());
            }
        }
        Collections.sort(indexes);
        return indexes;
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
        return Settings.builder()
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false) // disable merging so no segments will be upgraded
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 30) // speed up recoveries
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 30)
                .build();
    }

    void setupCluster() throws Exception {
        InternalTestCluster.Async<List<String>> replicas = internalCluster().startNodesAsync(1); // for replicas

        Path baseTempDir = createTempDir();
        // start single data path node
        Settings.Builder nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("single-path").toAbsolutePath())
            .put(Node.NODE_MASTER_SETTING.getKey(), false); // workaround for dangling index loading issue when node is master
        InternalTestCluster.Async<String> singleDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());

        // start multi data path node
        nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("multi-path1").toAbsolutePath() + "," + baseTempDir.resolve("multi-path2").toAbsolutePath())
            .put(Node.NODE_MASTER_SETTING.getKey(), false); // workaround for dangling index loading issue when node is master
        InternalTestCluster.Async<String> multiDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());

        // find single data path dir
        singleDataPathNodeName = singleDataPathNode.get();
        Path[] nodePaths = internalCluster().getInstance(NodeEnvironment.class, singleDataPathNodeName).nodeDataPaths();
        assertEquals(1, nodePaths.length);
        singleDataPath = nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER);
        assertFalse(Files.exists(singleDataPath));
        Files.createDirectories(singleDataPath);
        logger.info("--> Single data path: {}", singleDataPath);

        // find multi data path dirs
        multiDataPathNodeName = multiDataPathNode.get();
        nodePaths = internalCluster().getInstance(NodeEnvironment.class, multiDataPathNodeName).nodeDataPaths();
        assertEquals(2, nodePaths.length);
        multiDataPath = new Path[] {nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER),
                                   nodePaths[1].resolve(NodeEnvironment.INDICES_FOLDER)};
        assertFalse(Files.exists(multiDataPath[0]));
        assertFalse(Files.exists(multiDataPath[1]));
        Files.createDirectories(multiDataPath[0]);
        Files.createDirectories(multiDataPath[1]);
        logger.info("--> Multi data paths: {}, {}", multiDataPath[0], multiDataPath[1]);

        replicas.get(); // wait for replicas
    }

    void upgradeIndexFolder() throws Exception {
        final NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, singleDataPathNodeName);
        IndexFolderUpgrader.upgradeIndicesIfNeeded(Settings.EMPTY, nodeEnvironment);
        final NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class, multiDataPathNodeName);
        IndexFolderUpgrader.upgradeIndicesIfNeeded(Settings.EMPTY, nodeEnv);
    }

    String loadIndex(String indexFile) throws Exception {
        Path unzipDir = createTempDir();
        Path unzipDataDir = unzipDir.resolve("data");
        String indexName = indexFile.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");

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

        // the bwc scripts packs the indices under this path
        Path src = list[0].resolve("nodes/0/indices/" + indexName);
        assertTrue("[" + indexFile + "] missing index dir: " + src.toString(), Files.exists(src));

        if (randomBoolean()) {
            logger.info("--> injecting index [{}] into single data path", indexName);
            copyIndex(logger, src, indexName, singleDataPath);
        } else {
            logger.info("--> injecting index [{}] into multi data path", indexName);
            copyIndex(logger, src, indexName, multiDataPath);
        }
        return indexName;
    }

    void importIndex(String indexName) throws IOException {
        // force reloading dangling indices with a cluster state republish
        client().admin().cluster().prepareReroute().get();
        ensureGreen(indexName);
    }

    // randomly distribute the files from src over dests paths
    public static void copyIndex(final ESLogger logger, final Path src, final String indexName, final Path... dests) throws IOException {
        Path destinationDataPath = dests[randomInt(dests.length - 1)];
        for (Path dest : dests) {
            Path indexDir = dest.resolve(indexName);
            assertFalse(Files.exists(indexDir));
            Files.createDirectories(indexDir);
        }
        Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path relativeDir = src.relativize(dir);
                for (Path dest : dests) {
                    Path destDir = dest.resolve(indexName).resolve(relativeDir);
                    Files.createDirectories(destDir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().equals(IndexWriter.WRITE_LOCK_NAME)) {
                    // skip lock file, we don't need it
                    logger.trace("Skipping lock file: {}", file);
                    return FileVisitResult.CONTINUE;
                }

                Path relativeFile = src.relativize(file);
                Path destFile = destinationDataPath.resolve(indexName).resolve(relativeFile);
                logger.trace("--> Moving {} to {}", relativeFile, destFile);
                Files.move(file, destFile);
                assertFalse(Files.exists(file));
                assertTrue(Files.exists(destFile));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    void unloadIndex(String indexName) throws Exception {
        assertAcked(client().admin().indices().prepareDelete(indexName).get());
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allVersions()) {
            if (VersionUtils.isSnapshot(v)) continue;  // snapshots are unreleased, so there is no backcompat yet
            if (v.isAlpha()) continue; // no guarantees for alpha releases
            if (v.onOrBefore(Version.V_2_0_0_beta1)) continue; // we can only test back one major lucene version
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
        Version version = extractVersion(index);
        String indexName = loadIndex(index);
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
        assertUpgradeWorks(indexName, isLatestLuceneVersion(version));
        assertDeleteByQueryWorked(indexName, version);
        assertPositionIncrementGapDefaults(indexName, version);
        unloadIndex(indexName);
    }

    Version extractVersion(String index) {
        return Version.fromString(index.substring(index.indexOf('-') + 1, index.lastIndexOf('.')));
    }

    boolean isLatestLuceneVersion(Version version) {
        return version.luceneVersion.major == Version.CURRENT.luceneVersion.major &&
                version.luceneVersion.minor == Version.CURRENT.luceneVersion.minor;
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
                    assertEquals(indexCreated.luceneVersion, segment.version);
                }
            }
        }
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
        String stringValue = (String) bestHit.sourceAsMap().get("string");
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
        SearchResponse searchRsp = client().prepareSearch(indexName).addAggregation(AggregationBuilders.histogram("histo").field("long_sort").interval(10)).get();
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

    // #10067: create-bwc-index.py deleted any doc with long_sort:[10-20]
    void assertDeleteByQueryWorked(String indexName, Version version) throws Exception {
        if (version.onOrAfter(Version.V_2_0_0_beta1)) {
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

    void assertUpgradeWorks(String indexName, boolean alreadyLatest) throws Exception {
        if (alreadyLatest == false) {
            UpgradeIT.assertNotUpgraded(client(), indexName);
        }
        assertNoFailures(client().admin().indices().prepareUpgrade(indexName).get());
        UpgradeIT.assertUpgraded(client(), indexName);
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

        // the bwc scripts packs the indices under this path
        return list[0].resolve("nodes/0/");
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
            assertNotNull(globalFormat.loadLatestState(logger, nodeDir)); // no exception
            Path indexDir = nodeDir.resolve("indices").resolve(indexName);
            assertNotNull(indexFormat.loadLatestState(logger, indexDir)); // no exception
        }
    }
}
