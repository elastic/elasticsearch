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

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.MultiDataPathUpgrader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.action.admin.indices.upgrade.UpgradeTest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.index.merge.NoMergePolicyProvider;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
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
import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
@LuceneTestCase.Slow
public class OldIndexBackwardsCompatibilityTests extends ElasticsearchIntegrationTest {
    // TODO: test for proper exception on unsupported indexes (maybe via separate test?)
    // We have a 0.20.6.zip etc for this.

    List<String> indexes;
    static Path singleDataPath;
    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = new ArrayList<>();
        Path dir = getDataPath(".");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path.getFileName().toString());
            }
        }
        Collections.sort(indexes);
    }

    @AfterClass
    public static void tearDownStatics() {
        singleDataPath = null;
        multiDataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        return ImmutableSettings.builder()
                .put(Node.HTTP_ENABLED, true) // for _upgrade
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class) // disable merging so no segments will be upgraded
                .put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, 30) // increase recovery speed for small files
                .build();
    }

    void setupCluster() throws Exception {
        ListenableFuture<List<String>> replicas = internalCluster().startNodesAsync(1); // for replicas

        Path baseTempDir = createTempDir();
        // start single data path node
        ImmutableSettings.Builder nodeSettings = ImmutableSettings.builder()
            .put("path.data", baseTempDir.resolve("single-path").toAbsolutePath())
            .put("node.master", false); // workaround for dangling index loading issue when node is master
        ListenableFuture<String> singleDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());

        // start multi data path node
        nodeSettings = ImmutableSettings.builder()
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
        multiDataPath = new Path[] {nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER),
                                   nodePaths[1].resolve(NodeEnvironment.INDICES_FOLDER)};
        assertFalse(Files.exists(multiDataPath[0]));
        assertFalse(Files.exists(multiDataPath[1]));
        Files.createDirectories(multiDataPath[0]);
        Files.createDirectories(multiDataPath[1]);
        logger.info("--> Multi data paths: " + multiDataPath[0].toString() + ", " + multiDataPath[1].toString());

        replicas.get(); // wait for replicas
    }

    String loadIndex(String indexFile) throws Exception {
        Path unzipDir = createTempDir();
        Path unzipDataDir = unzipDir.resolve("data");
        String indexName = indexFile.replace(".zip", "").toLowerCase(Locale.ROOT);

        // decompress the index
        Path backwardsIndex = getDataPath(indexFile);
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
        final Iterable<NodeEnvironment> instances = internalCluster().getInstances(NodeEnvironment.class);
        for (NodeEnvironment nodeEnv : instances) { // upgrade multidata path
            MultiDataPathUpgrader.upgradeMultiDataPath(nodeEnv, logger);
        }
        // force reloading dangling indices with a cluster state republish
        client().admin().cluster().prepareReroute().get();
        ensureGreen(indexName);
        return indexName;
    }

    // randomly distribute the files from src over dests paths
    public static void copyIndex(final ESLogger logger, final Path src, final String indexName, final Path... dests) throws IOException {
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
                    logger.trace("Skipping lock file: " + file.toString());
                    return FileVisitResult.CONTINUE;
                }

                Path relativeFile = src.relativize(file);
                Path destFile = dests[randomInt(dests.length - 1)].resolve(indexName).resolve(relativeFile);
                logger.trace("--> Moving " + relativeFile.toString() + " to " + destFile.toString());
                Files.move(file, destFile);
                assertFalse(Files.exists(file));
                assertTrue(Files.exists(destFile));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    void unloadIndex(String indexName) throws Exception {
        ElasticsearchAssertions.assertAcked(client().admin().indices().prepareDelete(indexName).get());
        ElasticsearchAssertions.assertAllFilesClosed();
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

    void assertOldIndexWorks(String index) throws Exception {
        Version version = extractVersion(index);
        String indexName = loadIndex(index);
        assertIndexSanity(indexName);
        assertBasicSearchWorks(indexName);
        assertBasicAggregationWorks(indexName);
        assertRealtimeGetWorks(indexName);
        assertNewReplicasWork(indexName);
        assertUpgradeWorks(indexName, isLatestLuceneVersion(version));
        assertDeleteByQueryWorked(indexName, version);
        unloadIndex(indexName);
    }

    Version extractVersion(String index) {
        return Version.fromString(index.substring(index.indexOf('-') + 1, index.lastIndexOf('.')));
    }

    boolean isLatestLuceneVersion(Version version) {
        return version.luceneVersion.major == Version.CURRENT.luceneVersion.major &&
                version.luceneVersion.minor == Version.CURRENT.luceneVersion.minor;
    }

    void assertIndexSanity(String indexName) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices(indexName).get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals(indexName, getIndexResponse.indices()[0]);
        ensureYellow(indexName);
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
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.existsFilter("string")));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(numDocs, searchRsp.getHits().getTotalHits());

        logger.info("--> testing missing filter");
        // the field for the missing filter here needs to be different than the exists filter above, to avoid being found in the cache
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.missingFilter("long_sort")));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(0, searchRsp.getHits().getTotalHits());
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
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(ImmutableSettings.builder()
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

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(ImmutableSettings.builder()
                .put("refresh_interval", EngineConfig.DEFAULT_REFRESH_INTERVAL)
                .build()));
    }

    void assertNewReplicasWork(String indexName) throws Exception {
        final int numReplicas = 1;
        final long startTime = System.currentTimeMillis();
        logger.debug("--> creating [{}] replicas for index [{}]", numReplicas, indexName);
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", numReplicas)
        ).execute().actionGet());
        ensureGreen(TimeValue.timeValueMinutes(2), indexName);
        logger.debug("--> index [{}] is green, took [{}]", indexName, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
        logger.debug("--> recovery status:\n{}", XContentHelper.toString(client().admin().indices().prepareRecoveries(indexName).get()));

        // TODO: do something with the replicas! query? index?
    }

    // #10067: create-bwc-index.py deleted any doc with long_sort:[10-20]
    void assertDeleteByQueryWorked(String indexName, Version version) throws Exception {
        if (version.onOrBefore(Version.V_1_0_0_Beta2)) {
            // TODO: remove this once #10262 is fixed
            return;
        }
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.queryStringQuery("long_sort:[10 TO 20]"));
        assertEquals(0, searchReq.get().getHits().getTotalHits());
    }

    void assertUpgradeWorks(String indexName, boolean alreadyLatest) throws Exception {
        HttpRequestBuilder httpClient = httpClient();

        if (alreadyLatest == false) {
            UpgradeTest.assertNotUpgraded(httpClient, indexName);
        }
        UpgradeTest.runUpgrade(httpClient, indexName);
        UpgradeTest.assertUpgraded(httpClient, indexName);
    }

}
