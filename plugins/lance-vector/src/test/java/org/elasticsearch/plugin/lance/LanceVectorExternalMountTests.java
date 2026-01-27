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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LanceVectorExternalMountTests extends ESSingleNodeTestCase {

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

    public void testKnnBasicJoin() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
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
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("products").setMapping(mapping));
        ensureGreen("products");

        prepareIndex("products").setId("doc1").setSource("category", "a").get();
        prepareIndex("products").setId("doc2").setSource("category", "a").get();
        prepareIndex("products").setId("doc3").setSource("category", "b").get();
        indicesAdmin().prepareRefresh("products").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 3, 5, null, null, null);
        SearchResponse response = client().prepareSearch("products").setKnnSearch(List.of(knn)).setSize(3).get();
        try {
            assertThat(response.getHits().getHits().length, greaterThanOrEqualTo(1));
            assertThat(response.getHits().getHits().length, lessThanOrEqualTo(3));
            assertThat(response.getHits().getAt(0).getId(), equalTo("doc3"));
            assertTrue(response.getHits().getAt(0).getScore() > 0);
        } finally {
            response.decRef();
        }
    }

    public void testKnnWithFilter() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
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
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("products-filter").setMapping(mapping));
        ensureGreen("products-filter");

        prepareIndex("products-filter").setId("doc1").setSource("category", "a").get();
        prepareIndex("products-filter").setId("doc2").setSource("category", "a").get();
        prepareIndex("products-filter").setId("doc3").setSource("category", "b").get();
        indicesAdmin().prepareRefresh("products-filter").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 2, 5, null, null, null).addFilterQuery(
            termQuery("category", "a")
        );
        SearchResponse response = client().prepareSearch("products-filter").setKnnSearch(List.of(knn)).setSize(2).get();
        try {
            assertThat(response.getHits().getHits().length, lessThanOrEqualTo(2));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getSourceAsMap().get("category"), equalTo("a"));
            }
        } finally {
            response.decRef();
        }
    }

    public void testKnnWithDotProductSimilarity() throws Exception {
        String datasetUri = datasetUri("datasets/dotproduct.json");
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("tag")
            .field("type", "keyword")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 3)
            .field("similarity", "dot_product")
            .startObject("storage")
            .field("type", "external")
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("dot-product").setMapping(mapping));
        ensureGreen("dot-product");

        prepareIndex("dot-product").setId("doc1").setSource("tag", "x").get();
        prepareIndex("dot-product").setId("doc2").setSource("tag", "x").get();
        prepareIndex("dot-product").setId("doc3").setSource("tag", "x").get();
        prepareIndex("dot-product").setId("doc4").setSource("tag", "x").get();
        indicesAdmin().prepareRefresh("dot-product").get();

        float[] queryVector = new float[] { 1.0f, 2.0f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 4, 10, null, null, null);
        SearchResponse response = client().prepareSearch("dot-product").setKnnSearch(List.of(knn)).setSize(4).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(4));
            assertThat(response.getHits().getAt(0).getId(), equalTo("doc1"));
            assertThat(response.getHits().getAt(3).getId(), equalTo("doc4"));
        } finally {
            response.decRef();
        }
    }

    public void testKnnWithKLargerThanCandidates() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 3)
            .startObject("storage")
            .field("type", "external")
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("large-k").setMapping(mapping));
        ensureGreen("large-k");

        prepareIndex("large-k").setId("doc1").setSource("data", "test").get();
        prepareIndex("large-k").setId("doc2").setSource("data", "test").get();
        prepareIndex("large-k").setId("doc3").setSource("data", "test").get();
        indicesAdmin().prepareRefresh("large-k").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 100, 100, null, null, null);
        SearchResponse response = client().prepareSearch("large-k").setKnnSearch(List.of(knn)).setSize(100).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(3));
        } finally {
            response.decRef();
        }
    }

    public void testKnnFilterExcludesAllCandidates() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
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
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("filter-all").setMapping(mapping));
        ensureGreen("filter-all");

        prepareIndex("filter-all").setId("doc1").setSource("category", "a").get();
        prepareIndex("filter-all").setId("doc2").setSource("category", "a").get();
        prepareIndex("filter-all").setId("doc3").setSource("category", "a").get();
        indicesAdmin().prepareRefresh("filter-all").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 2, 5, null, null, null).addFilterQuery(
            termQuery("category", "nonexistent")
        );
        SearchResponse response = client().prepareSearch("filter-all").setKnnSearch(List.of(knn)).setSize(2).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(0));
        } finally {
            response.decRef();
        }
    }

    public void testKnnWithMissingLuceneDocuments() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 3)
            .startObject("storage")
            .field("type", "external")
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("missing-docs").setMapping(mapping));
        ensureGreen("missing-docs");

        prepareIndex("missing-docs").setId("doc1").setSource("data", "test").get();
        prepareIndex("missing-docs").setId("doc3").setSource("data", "test").get();
        indicesAdmin().prepareRefresh("missing-docs").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 10, 10, null, null, null);
        SearchResponse response = client().prepareSearch("missing-docs").setKnnSearch(List.of(knn)).setSize(10).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(2));
        } finally {
            response.decRef();
        }
    }

    public void testKnnHybridSearchWithBoost() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .endObject()
            .startObject("embedding")
            .field("type", "lance_vector")
            .field("dims", 3)
            .startObject("storage")
            .field("type", "external")
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("hybrid").setMapping(mapping));
        ensureGreen("hybrid");

        prepareIndex("hybrid").setId("doc1").setSource("title", "quick brown fox").get();
        prepareIndex("hybrid").setId("doc2").setSource("title", "lazy dog").get();
        prepareIndex("hybrid").setId("doc3").setSource("title", "quick rabbit").get();
        indicesAdmin().prepareRefresh("hybrid").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 3, 5, null, null, null);
        SearchResponse response = client().prepareSearch("hybrid").setKnnSearch(List.of(knn)).setSize(3).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(3));
            assertTrue(response.getHits().getAt(0).getScore() > 0);
        } finally {
            response.decRef();
        }
    }

    public void testKnnWithSingleResult() throws Exception {
        String datasetUri = datasetUri("datasets/simple.json");
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
            .field("uri", datasetUri)
            .field("lance_id_column", "id")
            .field("lance_vector_column", "vector")
            .field("read_only", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("single-result").setMapping(mapping));
        ensureGreen("single-result");

        prepareIndex("single-result").setId("doc1").setSource("category", "a").get();
        prepareIndex("single-result").setId("doc2").setSource("category", "b").get();
        prepareIndex("single-result").setId("doc3").setSource("category", "c").get();
        indicesAdmin().prepareRefresh("single-result").get();

        float[] queryVector = new float[] { 0.9f, 0.1f, 0.0f };
        KnnSearchBuilder knn = new KnnSearchBuilder("embedding", queryVector, 10, 10, null, null, null).addFilterQuery(
            termQuery("category", "c")
        );
        SearchResponse response = client().prepareSearch("single-result").setKnnSearch(List.of(knn)).setSize(10).get();
        try {
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getId(), equalTo("doc3"));
        } finally {
            response.decRef();
        }
    }

    private String datasetUri(String resource) {
        Path path = getDataPath(resource);
        return path.toUri().toString();
    }
}
