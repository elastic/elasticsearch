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
package org.elasticsearch.percolator;

import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class PercolatorFacetsAndAggregationsIT extends ESIntegTestCase {

    @Test
    // Just test the integration with facets and aggregations, not the facet and aggregation functionality!
    public void testFacetsAndAggregations() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "field1", "type=string", "field2", "type=string"));
        ensureGreen();

        int numQueries = scaledRandomIntBetween(250, 500);
        int numUniqueQueries = between(1, numQueries / 2);
        String[] values = new String[numUniqueQueries];
        for (int i = 0; i < values.length; i++) {
            values[i] = "value" + i;
        }
        int[] expectedCount = new int[numUniqueQueries];

        logger.info("--> registering {} queries", numQueries);
        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            expectedCount[i % numUniqueQueries]++;
            QueryBuilder queryBuilder = matchQuery("field1", value);
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", queryBuilder).field("field2", "b").endObject()).execute()
                    .actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate().setIndices("test").setDocumentType("type")
                    .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", value).endObject()));

            SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
            percolateRequestBuilder.addAggregation(AggregationBuilders.terms("a").field("field2").collectMode(aggCollectionMode));

            if (randomBoolean()) {
                percolateRequestBuilder.setPercolateQuery(matchAllQuery());
            }
            if (randomBoolean()) {
                percolateRequestBuilder.setScore(true);
            } else {
                percolateRequestBuilder.setSortByScore(true).setSize(numQueries);
            }

            boolean countOnly = randomBoolean();
            if (countOnly) {
                percolateRequestBuilder.setOnlyCount(countOnly);
            }

            PercolateResponse response = percolateRequestBuilder.execute().actionGet();
            assertMatchCount(response, expectedCount[i % numUniqueQueries]);
            if (!countOnly) {
                assertThat(response.getMatches(), arrayWithSize(expectedCount[i % numUniqueQueries]));
            }

            List<Aggregation> aggregations = response.getAggregations().asList();
            assertThat(aggregations.size(), equalTo(1));
            assertThat(aggregations.get(0).getName(), equalTo("a"));
            List<Terms.Bucket> buckets = new ArrayList<>(((Terms) aggregations.get(0)).getBuckets());
            assertThat(buckets.size(), equalTo(1));
            assertThat(buckets.get(0).getKeyAsString(), equalTo("b"));
            assertThat(buckets.get(0).getDocCount(), equalTo((long) expectedCount[i % values.length]));
        }
    }

    @Test
    // Just test the integration with facets and aggregations, not the facet and aggregation functionality!
    public void testAggregationsAndPipelineAggregations() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "field1", "type=string", "field2", "type=string"));
        ensureGreen();

        int numQueries = scaledRandomIntBetween(250, 500);
        int numUniqueQueries = between(1, numQueries / 2);
        String[] values = new String[numUniqueQueries];
        for (int i = 0; i < values.length; i++) {
            values[i] = "value" + i;
        }
        int[] expectedCount = new int[numUniqueQueries];

        logger.info("--> registering {} queries", numQueries);
        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            expectedCount[i % numUniqueQueries]++;
            QueryBuilder queryBuilder = matchQuery("field1", value);
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", queryBuilder).field("field2", "b").endObject()).execute()
                    .actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate().setIndices("test").setDocumentType("type")
                    .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", value).endObject()));

            SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
            percolateRequestBuilder.addAggregation(AggregationBuilders.terms("a").field("field2").collectMode(aggCollectionMode));

            if (randomBoolean()) {
                percolateRequestBuilder.setPercolateQuery(matchAllQuery());
            }
            if (randomBoolean()) {
                percolateRequestBuilder.setScore(true);
            } else {
                percolateRequestBuilder.setSortByScore(true).setSize(numQueries);
            }

            boolean countOnly = randomBoolean();
            if (countOnly) {
                percolateRequestBuilder.setOnlyCount(countOnly);
            }

            percolateRequestBuilder.addAggregation(PipelineAggregatorBuilders.maxBucket("max_a").setBucketsPaths("a>_count"));

            PercolateResponse response = percolateRequestBuilder.execute().actionGet();
            assertMatchCount(response, expectedCount[i % numUniqueQueries]);
            if (!countOnly) {
                assertThat(response.getMatches(), arrayWithSize(expectedCount[i % numUniqueQueries]));
            }

            Aggregations aggregations = response.getAggregations();
            assertThat(aggregations.asList().size(), equalTo(2));
            Terms terms = aggregations.get("a");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("a"));
            List<Terms.Bucket> buckets = new ArrayList<>(terms.getBuckets());
            assertThat(buckets.size(), equalTo(1));
            assertThat(buckets.get(0).getKeyAsString(), equalTo("b"));
            assertThat(buckets.get(0).getDocCount(), equalTo((long) expectedCount[i % values.length]));

            InternalBucketMetricValue maxA = aggregations.get("max_a");
            assertThat(maxA, notNullValue());
            assertThat(maxA.getName(), equalTo("max_a"));
            assertThat(maxA.value(), equalTo((double) expectedCount[i % values.length]));
            assertThat(maxA.keys(), equalTo(new String[] { "b" }));
        }
    }

    @Test
    public void testSignificantAggs() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();
        PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate().setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value").endObject()))
                .addAggregation(AggregationBuilders.significantTerms("a").field("field2"));
        PercolateResponse response = percolateRequestBuilder.get();
        assertNoFailures(response);
    }

    @Test
    public void testSingleShardAggregations() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("SETTING_NUMBER_OF_SHARDS", 1))
                .addMapping("type", "field1", "type=string", "field2", "type=string"));
        ensureGreen();

        int numQueries = scaledRandomIntBetween(250, 500);

        logger.info("--> registering {} queries", numQueries);
        for (int i = 0; i < numQueries; i++) {
            String value = "value0";
            QueryBuilder queryBuilder = matchQuery("field1", value);
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", queryBuilder).field("field2", i % 3 == 0 ? "b" : "a").endObject())
                    .execute()
                    .actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        for (int i = 0; i < numQueries; i++) {
            String value = "value0";
            PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate().setIndices("test").setDocumentType("type")
                    .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", value).endObject()));

            SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
            percolateRequestBuilder.addAggregation(AggregationBuilders.terms("terms").field("field2").collectMode(aggCollectionMode)
                    .order(Order.term(true)).shardSize(2).size(1));

            if (randomBoolean()) {
                percolateRequestBuilder.setPercolateQuery(matchAllQuery());
            }
            if (randomBoolean()) {
                percolateRequestBuilder.setScore(true);
            } else {
                percolateRequestBuilder.setSortByScore(true).setSize(numQueries);
            }

            boolean countOnly = randomBoolean();
            if (countOnly) {
                percolateRequestBuilder.setOnlyCount(countOnly);
            }

            percolateRequestBuilder.addAggregation(PipelineAggregatorBuilders.maxBucket("max_terms").setBucketsPaths("terms>_count"));

            PercolateResponse response = percolateRequestBuilder.execute().actionGet();
            assertMatchCount(response, numQueries);
            if (!countOnly) {
                assertThat(response.getMatches(), arrayWithSize(numQueries));
            }

            Aggregations aggregations = response.getAggregations();
            assertThat(aggregations.asList().size(), equalTo(2));
            Terms terms = aggregations.get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            List<Terms.Bucket> buckets = new ArrayList<>(terms.getBuckets());
            assertThat(buckets.size(), equalTo(1));
            assertThat(buckets.get(0).getKeyAsString(), equalTo("a"));

            InternalBucketMetricValue maxA = aggregations.get("max_terms");
            assertThat(maxA, notNullValue());
            assertThat(maxA.getName(), equalTo("max_terms"));
            assertThat(maxA.keys(), equalTo(new String[] { "a" }));
        }
    }
}
