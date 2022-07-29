/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests the Sampler aggregation
 */
@ESIntegTestCase.SuiteScopeTestCase
public class DiversifiedSamplerIT extends ESIntegTestCase {

    public static final int NUM_SHARDS = 2;

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SamplerAggregator.ExecutionMode.values()).toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("author", "type=keyword", "name", "type=keyword", "genre", "type=keyword", "price", "type=float")
        );
        createIndex("idx_unmapped");
        // idx_unmapped_author is same as main index but missing author field
        assertAcked(
            prepareCreate("idx_unmapped_author").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("name", "type=keyword", "genre", "type=keyword", "price", "type=float")
        );

        ensureGreen();
        String data[] = {
            // "id,cat,name,price,inStock,author_t,series_t,sequence_i,genre_s",
            "0553573403,book,A Game of Thrones,7.99,true,George R.R. Martin,A Song of Ice and Fire,1,fantasy",
            "0553579908,book,A Clash of Kings,7.99,true,George R.R. Martin,A Song of Ice and Fire,2,fantasy",
            "055357342X,book,A Storm of Swords,7.99,true,George R.R. Martin,A Song of Ice and Fire,3,fantasy",
            "0553293354,book,Foundation,17.99,true,Isaac Asimov,Foundation Novels,1,scifi",
            "0812521390,book,The Black Company,6.99,false,Glen Cook,The Chronicles of The Black Company,1,fantasy",
            "0812550706,book,Ender's Game,6.99,true,Orson Scott Card,Ender,1,scifi",
            "0441385532,book,Jhereg,7.95,false,Steven Brust,Vlad Taltos,1,fantasy",
            "0380014300,book,Nine Princes In Amber,6.99,true,Roger Zelazny,the Chronicles of Amber,1,fantasy",
            "0805080481,book,The Book of Three,5.99,true,Lloyd Alexander,The Chronicles of Prydain,1,fantasy",
            "080508049X,book,The Black Cauldron,5.99,true,Lloyd Alexander,The Chronicles of Prydain,2,fantasy"

        };

        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split(",");
            client().prepareIndex("test")
                .setId("" + i)
                .setSource("author", parts[5], "name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3]))
                .get();
            client().prepareIndex("idx_unmapped_author")
                .setId("" + i)
                .setSource("name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3]))
                .get();
        }
        client().admin().indices().refresh(new RefreshRequest("test")).get();
    }

    public void testIssue10719() throws Exception {
        // Tests that we can refer to nested elements under a sample in a path
        // statement
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(
                terms("genres").field("genre")
                    .order(BucketOrder.aggregation("sample>max_price.value", asc))
                    .subAggregation(sampler("sample").shardSize(100).subAggregation(max("max_price").field("price")))
            )
            .get();
        assertSearchResponse(response);
        Terms genres = response.getAggregations().get("genres");
        Collection<? extends Bucket> genreBuckets = genres.getBuckets();
        // For this test to be useful we need >1 genre bucket to compare
        assertThat(genreBuckets.size(), greaterThan(1));
        double lastMaxPrice = asc ? Double.MIN_VALUE : Double.MAX_VALUE;
        for (Terms.Bucket genreBucket : genres.getBuckets()) {
            Sampler sample = genreBucket.getAggregations().get("sample");
            Max maxPriceInGenre = sample.getAggregations().get("max_price");
            double price = maxPriceInGenre.value();
            if (asc) {
                assertThat(price, greaterThanOrEqualTo(lastMaxPrice));
            } else {
                assertThat(price, lessThanOrEqualTo(lastMaxPrice));
            }
            lastMaxPrice = price;
        }

    }

    public void testSimpleDiversity() throws Exception {
        int MAX_DOCS_PER_AUTHOR = 1;
        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100);
        sampleAgg.field("author").maxDocsPerValue(MAX_DOCS_PER_AUTHOR).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));
        SearchResponse response = client().prepareSearch("test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new TermQueryBuilder("genre", "fantasy"))
            .setFrom(0)
            .setSize(60)
            .addAggregation(sampleAgg)
            .get();
        assertSearchResponse(response);
        Sampler sample = response.getAggregations().get("sample");
        Terms authors = sample.getAggregations().get("authors");
        List<? extends Bucket> testBuckets = authors.getBuckets();

        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket.getDocCount(), lessThanOrEqualTo((long) NUM_SHARDS * MAX_DOCS_PER_AUTHOR));
        }
    }

    public void testNestedDiversity() throws Exception {
        // Test multiple samples gathered under buckets made by a parent agg
        int MAX_DOCS_PER_AUTHOR = 1;
        TermsAggregationBuilder rootTerms = terms("genres").field("genre");

        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100);
        sampleAgg.field("author").maxDocsPerValue(MAX_DOCS_PER_AUTHOR).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));

        rootTerms.subAggregation(sampleAgg);
        SearchResponse response = client().prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH).addAggregation(rootTerms).get();
        assertSearchResponse(response);
        Terms genres = response.getAggregations().get("genres");
        List<? extends Bucket> genreBuckets = genres.getBuckets();
        for (Terms.Bucket genreBucket : genreBuckets) {
            Sampler sample = genreBucket.getAggregations().get("sample");
            Terms authors = sample.getAggregations().get("authors");
            List<? extends Bucket> testBuckets = authors.getBuckets();

            for (Terms.Bucket testBucket : testBuckets) {
                assertThat(testBucket.getDocCount(), lessThanOrEqualTo((long) NUM_SHARDS * MAX_DOCS_PER_AUTHOR));
            }
        }
    }

    public void testNestedSamples() throws Exception {
        // Test samples nested under samples
        int MAX_DOCS_PER_AUTHOR = 1;
        int MAX_DOCS_PER_GENRE = 2;
        DiversifiedAggregationBuilder rootSample = new DiversifiedAggregationBuilder("genreSample").shardSize(100)
            .field("genre")
            .maxDocsPerValue(MAX_DOCS_PER_GENRE);

        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100);
        sampleAgg.field("author").maxDocsPerValue(MAX_DOCS_PER_AUTHOR).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));
        sampleAgg.subAggregation(terms("genres").field("genre"));

        rootSample.subAggregation(sampleAgg);
        SearchResponse response = client().prepareSearch("test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(rootSample)
            .get();
        assertSearchResponse(response);
        Sampler genreSample = response.getAggregations().get("genreSample");
        Sampler sample = genreSample.getAggregations().get("sample");

        Terms genres = sample.getAggregations().get("genres");
        List<? extends Bucket> testBuckets = genres.getBuckets();
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket.getDocCount(), lessThanOrEqualTo((long) NUM_SHARDS * MAX_DOCS_PER_GENRE));
        }

        Terms authors = sample.getAggregations().get("authors");
        testBuckets = authors.getBuckets();
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket.getDocCount(), lessThanOrEqualTo((long) NUM_SHARDS * MAX_DOCS_PER_AUTHOR));
        }
    }

    public void testPartiallyUnmappedDiversifyField() throws Exception {
        // One of the indexes is missing the "author" field used for
        // diversifying results
        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100)
            .field("author")
            .maxDocsPerValue(1);
        sampleAgg.subAggregation(terms("authors").field("author"));
        SearchResponse response = client().prepareSearch("idx_unmapped_author", "test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new TermQueryBuilder("genre", "fantasy"))
            .setFrom(0)
            .setSize(60)
            .addAggregation(sampleAgg)
            .get();
        assertSearchResponse(response);
        Sampler sample = response.getAggregations().get("sample");
        assertThat(sample.getDocCount(), greaterThan(0L));
        Terms authors = sample.getAggregations().get("authors");
        assertThat(authors.getBuckets().size(), greaterThan(0));
    }

    public void testWhollyUnmappedDiversifyField() throws Exception {
        // All of the indices are missing the "author" field used for diversifying results
        int MAX_DOCS_PER_AUTHOR = 1;
        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100);
        sampleAgg.field("author").maxDocsPerValue(MAX_DOCS_PER_AUTHOR).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));
        SearchResponse response = client().prepareSearch("idx_unmapped", "idx_unmapped_author")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new TermQueryBuilder("genre", "fantasy"))
            .setFrom(0)
            .setSize(60)
            .addAggregation(sampleAgg)
            .get();
        assertSearchResponse(response);
        Sampler sample = response.getAggregations().get("sample");
        assertThat(sample.getDocCount(), equalTo(0L));
        Terms authors = sample.getAggregations().get("authors");
        assertNull(authors);
    }

    public void testRidiculousSizeDiversity() throws Exception {
        int MAX_DOCS_PER_AUTHOR = 1;
        DiversifiedAggregationBuilder sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(Integer.MAX_VALUE);
        sampleAgg.field("author").maxDocsPerValue(MAX_DOCS_PER_AUTHOR).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));
        SearchResponse response = client().prepareSearch("test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new TermQueryBuilder("genre", "fantasy"))
            .setFrom(0)
            .setSize(60)
            .addAggregation(sampleAgg)
            .get();
        assertSearchResponse(response);

        sampleAgg = new DiversifiedAggregationBuilder("sample").shardSize(100);
        sampleAgg.field("author").maxDocsPerValue(Integer.MAX_VALUE).executionHint(randomExecutionHint());
        sampleAgg.subAggregation(terms("authors").field("author"));
        response = client().prepareSearch("test")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new TermQueryBuilder("genre", "fantasy"))
            .setFrom(0)
            .setSize(60)
            .addAggregation(sampleAgg)
            .get();
        assertSearchResponse(response);
    }

}
