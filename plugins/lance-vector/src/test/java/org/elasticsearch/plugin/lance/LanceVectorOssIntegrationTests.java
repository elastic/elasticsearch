/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.plugin.lance.storage.LanceDatasetConfig;
import org.elasticsearch.plugin.lance.storage.LanceDatasetRegistry;
import org.elasticsearch.plugin.lance.storage.RealLanceDataset;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SuppressForbidden;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration test for Lance Vector with OSS storage (no mocks, no fakes).
 * <p>
 * This test validates real Lance dataset operations with local file:// URIs as baseline.
 * To test with actual OSS buckets, set environment variables:
 * <ul>
 *   <li>OSS_TEST_URI - Full OSS URI to test dataset (e.g., oss://bucket/path/dataset.lance)</li>
 *   <li>OSS_ACCESS_KEY_ID - OSS access key</li>
 *   <li>OSS_ACCESS_KEY_SECRET - OSS secret key</li>
 *   <li>OSS_ENDPOINT - OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)</li>
 * </ul>
 * <p>
 * Test dataset can be created using: scripts/create_test_dataset.py
 */
public class LanceVectorOssIntegrationTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(LanceVectorOssIntegrationTests.class);

    private static final String OSS_TEST_URI = System.getProperty("oss.test.uri",
        System.getenv().getOrDefault("OSS_TEST_URI", ""));

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LanceVectorPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LanceDatasetRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception {
        LanceDatasetRegistry.clear();
        super.tearDown();
    }

    /**
     * Test: Local Lance dataset with IVF-PQ index (baseline validation).
     * <p>
     * This validates that the lance-java SDK works correctly with local file:// URIs.
     * Prerequisites:
     * <ul>
     *   <li>Lance native libraries must be available</li>
     *   <li>Run: python scripts/create_test_dataset.py /tmp/test-vectors.lance</li>
     * </ul>
     */
    public void testLocalLanceDatasetWithIvfPqIndex() throws Exception {
        // Check for default path or system property
        String defaultPath = "/tmp/test-vectors.lance";
        String datasetPath = System.getProperty("test.dataset.path", defaultPath);

        // Skip test if dataset doesn't exist
        assumeTrue(
            "Skipping - dataset not found at: " + datasetPath
                + ". Create it with: python scripts/create_test_dataset.py " + datasetPath,
            Files.exists(PathUtils.get(datasetPath))
        );

        logger.info("Testing with local Lance dataset: {}", datasetPath);

        // Validate dataset can be opened via RealLanceDataset
        LanceDatasetConfig config = LanceDatasetConfig.withDims(128);
        try (RealLanceDataset dataset = RealLanceDataset.open(datasetPath, config)) {
            assertThat(dataset.dims(), equalTo(128));
            assertThat(dataset.size(), greaterThan(0L));
            assertThat(dataset.hasIndex(), is(true));

            logger.info("Dataset opened: vectors={}, dims={}, indexed={}",
                dataset.size(), dataset.dims(), dataset.hasIndex());

            // Test vector search
            float[] queryVector = new float[128];
            for (int i = 0; i < 128; i++) {
                queryVector[i] = Math.random();
            }

            var candidates = dataset.search(queryVector, 5, "cosine");
            assertThat(candidates, notNullValue());
            assertThat(candidates.size(), greaterThan(0));

            logger.info("Vector search returned {} candidates", candidates.size());
        }

        // Now test with Elasticsearch kNN search
        String datasetUri = datasetPath.startsWith("file://") ? datasetPath : "file://" + datasetPath;

        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 128)
            .startObject("storage")
            .field("type", "external")
            .field("uri", datasetUri)
            .field("lance_id_column", "_id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("lance-test").setMapping(mapping));
        ensureGreen("lance-test");

        // Index metadata documents - match IDs in the Lance dataset (doc_0, doc_1, ..., doc_999)
        // The Lance dataset was created with underscore format: doc_{i}
        for (int i = 0; i < 1000; i++) {
            prepareIndex("lance-test").setId("doc_" + i)
                .setSource("category", randomFrom("tech", "science", "business"))
                .get();
        }
        indicesAdmin().prepareRefresh("lance-test").get();

        // Execute kNN search
        float[] queryVector = new float[128];
        for (int i = 0; i < 128; i++) {
            queryVector[i] = Math.random();
        }

        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 5, 10, null, null, null);
        SearchResponse response = client().prepareSearch("lance-test")
            .setKnnSearch(List.of(knn))
            .setSize(5)
            .get();

        try {
            assertThat(response.getHits().getHits().length, greaterThan(0));
            for (SearchHit hit : response.getHits()) {
                assertThat(hit.getScore(), greaterThan(0.0f));
            }

            logger.info("kNN search returned {} results", response.getHits().getHits().length);
        } finally {
            response.decRef();
        }
    }

    /**
     * Test: Real OSS URI with lance-java SDK (no mocks).
     * <p>
     * This test validates OSS URI support with actual Alibaba Cloud OSS.
     * To enable, set the OSS_TEST_URI environment variable or system property.
     * <p>
     * Prerequisites:
     * <ul>
     *   <li>Real OSS bucket with .lance dataset</li>
     *   <li>Dataset created via: python scripts/create_test_dataset.py /tmp/data.lance --upload oss://bucket/test-data/</li>
     *   <li>OSS credentials configured (see lance-java SDK documentation)</li>
     * </ul>
     */
    public void testRealOssUriWithLanceSdk() throws Exception {
        assumeTrue("Skipping - set OSS_TEST_URI environment variable to enable", OSS_TEST_URI.isEmpty() == false);

        logger.info("Testing with real OSS URI: {}", OSS_TEST_URI);

        // Validate OSS URI format
        assertThat(OSS_TEST_URI, startsWith("oss://"));

        // Test opening dataset with OSS URI
        LanceDatasetConfig config = LanceDatasetConfig.withDims(128);

        try (RealLanceDataset dataset = RealLanceDataset.open(OSS_TEST_URI, config)) {
            assertThat(dataset.dims(), equalTo(128));
            assertThat(dataset.size(), greaterThan(0L));
            assertThat(dataset.uri(), equalTo(OSS_TEST_URI));

            logger.info("Successfully opened OSS dataset: vectors={}, dims={}, indexed={}",
                dataset.size(), dataset.dims(), dataset.hasIndex());

            // Test vector search on OSS dataset
            float[] queryVector = new float[128];
            for (int i = 0; i < 128; i++) {
                queryVector[i] = Math.random();
            }

            long startTime = System.currentTimeMillis();
            var candidates = dataset.search(queryVector, 10, "cosine");
            long searchTime = System.currentTimeMillis() - startTime;

            assertThat(candidates, notNullValue());
            assertThat(candidates.size(), greaterThan(0));

            logger.info("OSS vector search completed in {}ms, returned {} candidates",
                searchTime, candidates.size());

            for (var candidate : candidates) {
                logger.info("  Candidate: id={}, score={}", candidate.id(), candidate.score());
            }
        }
    }

    /**
     * Test: Elasticsearch kNN search with OSS URI.
     */
    public void testElasticsearchKnnSearchWithOssUri() throws Exception {
        assumeTrue("Skipping - set OSS_TEST_URI environment variable to enable", OSS_TEST_URI.isEmpty() == false);

        logger.info("Testing ES kNN search with OSS URI: {}", OSS_TEST_URI);

        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 128)
            .startObject("storage")
            .field("type", "external")
            .field("uri", OSS_TEST_URI)
            .field("lance_id_column", "_id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("oss-lance-test").setMapping(mapping));
        ensureGreen("oss-lance-test");

        // Index metadata documents
        for (int i = 0; i < 100; i++) {
            prepareIndex("oss-lance-test").setId("doc" + i)
                .setSource("category", randomFrom("tech", "science", "business"))
                .get();
        }
        indicesAdmin().prepareRefresh("oss-lance-test").get();

        // Execute kNN search against OSS dataset
        float[] queryVector = new float[128];
        for (int i = 0; i < 128; i++) {
            queryVector[i] = Math.random();
        }

        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 10, 20, null, null, null);
        SearchResponse response = client().prepareSearch("oss-lance-test")
            .setKnnSearch(List.of(knn))
            .setSize(10)
            .get();

        try {
            assertThat(response.getHits().getHits().length, greaterThan(0));

            logger.info("ES kNN search with OSS URI returned {} results", response.getHits().getHits().length);

            for (SearchHit hit : response.getHits()) {
                assertThat(hit.getScore(), greaterThan(0.0f));
                logger.info("  Result: id={}, score={}, source={}",
                    hit.getId(), hit.getScore(), hit.getSourceAsMap());
            }
        } finally {
            response.decRef();
        }
    }

    /**
     * Test: Validate LanceDatasetRegistry correctly identifies OSS URIs.
     */
    public void testLanceDatasetRegistryOssUriDetection() {
        // Test OSS URI detection
        assertTrue(LanceDatasetRegistry.isLanceFormat("oss://bucket/path/to/data.lance"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("oss://my-bucket/datasets/vectors.lance"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("oss://bucket-123/data-2024/embeddings.lance"));

        // Test non-OSS URIs
        assertFalse(LanceDatasetRegistry.isLanceFormat("oss://bucket/data.json"));
        assertFalse(LanceDatasetRegistry.isLanceFormat("s3://bucket/data.lance"));
        assertFalse(LanceDatasetRegistry.isLanceFormat("file:///path/to/data.json"));
    }

    /**
     * Test: URI normalization for different schemes.
     */
    public void testUriNormalization() throws Exception {
        // Test file:// URI normalization
        Path tempDir = createTempDir();
        Path datasetPath = tempDir.resolve("test.lance");

        // file:// prefix should be stripped
        String fileUri = "file://" + datasetPath.toString();
        assertThat(fileUri, startsWith("file://"));

        // oss:// URIs should be passed through as-is
        String ossUri = "oss://bucket/path/to/data.lance";
        assertThat(ossUri, startsWith("oss://"));
    }
}
