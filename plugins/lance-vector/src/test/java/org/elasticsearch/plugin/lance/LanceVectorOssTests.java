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
import org.elasticsearch.plugin.lance.storage.FakeLanceDatasetRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test for Lance Vector with Alibaba Cloud OSS storage.
 *
 * This test demonstrates how to configure lance_vector fields with OSS URIs.
 * To run with a real OSS bucket, set the OSS_TEST_BUCKET and OSS_TEST_DATASET
 * environment variables and ensure ~/.oss/credentials.json is configured.
 */
public class LanceVectorOssTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LanceVectorPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FakeLanceDatasetRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception {
        FakeLanceDatasetRegistry.clear();
        super.tearDown();
    }

    public void testOssUriMapping() throws Exception {
        // Test OSS URI format in mapping
        String ossUri = "oss://my-vector-bucket/datasets/embeddings.json";
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 768)
            .startObject("storage")
            .field("type", "external")
            .field("uri", ossUri)
            .field("lance_id_column", "document_id")
            .field("lance_vector_column", "embedding")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        // This should succeed - URI validation doesn't perform actual OSS access at mount time
        assertAcked(indicesAdmin().prepareCreate("oss-vectors").setMapping(mapping));
        ensureGreen("oss-vectors");
    }

    /**
     * Test with local dataset using OSS-like URI format for demo purposes.
     * In a real POC, this would point to an actual OSS bucket.
     */
    public void testOssLikeDataset() throws Exception {
        // Use local file but with OSS-like path structure for demo
        String localDatasetUri = datasetUri("oss-demo/vectors.json");

        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 3)
            .startObject("storage")
            .field("type", "external")
            .field("uri", localDatasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("oss-demo").setMapping(mapping));
        ensureGreen("oss-demo");

        // Index some metadata docs (these would exist in your ES index)
        prepareIndex("oss-demo").setId("doc1").setSource("category", "tech").get();
        prepareIndex("oss-demo").setId("doc2").setSource("category", "science").get();
        prepareIndex("oss-demo").setId("doc3").setSource("category", "tech").get();
        indicesAdmin().prepareRefresh("oss-demo").get();

        // Query with kNN + filter (use addFilterQuery to filter kNN results)
        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 2, 5, null, null, null).addFilterQuery(
            termQuery("category", "tech")
        );
        SearchResponse response = client().prepareSearch("oss-demo").setKnnSearch(List.of(knn)).setSize(2).get();
        try {
            // Should return only docs matching the filter
            assertThat(response.getHits().getHits().length, equalTo(2));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getSourceAsMap().get("category"), equalTo("tech"));
            }
        } finally {
            response.decRef();
        }
    }

    private String datasetUri(String resource) {
        Path path = getDataPath(resource);
        return path.toUri().toString();
    }
}
