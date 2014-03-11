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

package org.elasticsearch.search.aggregations;

import com.carrotsearch.hppc.IntOpenHashSet;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Additional tests that aim at testing more complex aggregation trees on larger random datasets, so that things like
 * the growth of dynamic arrays is tested.
 */
public class RandomTests extends ElasticsearchIntegrationTest {

    // Make sure that unordered, reversed, disjoint and/or overlapping ranges are supported
    // Duel with filters
    public void testRandomRanges() throws Exception {
        final int numDocs = atLeast(1000);
        final double[][] docs = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = randomInt(5);
            docs[i] = new double[numValues];
            for (int j = 0; j < numValues; ++j) {
                docs[i][j] = randomDouble() * 100;
            }
        }

        createIndex("idx");
        for (int i = 0; i < docs.length; ++i) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .startArray("values");
            for (int j = 0; j < docs[i].length; ++j) {
                source = source.value(docs[i][j]);
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx", "type").setSource(source).execute().actionGet();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenient()).execute().get());

        final int numRanges = randomIntBetween(1, 20);
        final double[][] ranges = new double[numRanges][];
        for (int i = 0; i < ranges.length; ++i) {
            switch (randomInt(2)) {
            case 0:
                ranges[i] = new double[] { Double.NEGATIVE_INFINITY, randomInt(100) };
                break;
            case 1:
                ranges[i] = new double[] { randomInt(100), Double.POSITIVE_INFINITY };
                break;
            case 2:
                ranges[i] = new double[] { randomInt(100), randomInt(100) };
                break;
            default:
                throw new AssertionError();
            }
        }

        RangeBuilder query = range("range").field("values");
        for (int i = 0; i < ranges.length; ++i) {
            String key = Integer.toString(i);
            if (ranges[i][0] == Double.NEGATIVE_INFINITY) {
                query.addUnboundedTo(key, ranges[i][1]);
            } else if (ranges[i][1] == Double.POSITIVE_INFINITY) {
                query.addUnboundedFrom(key, ranges[i][0]);
            } else {
                query.addRange(key, ranges[i][0], ranges[i][1]);
            }
        }

        SearchRequestBuilder reqBuilder = client().prepareSearch("idx").addAggregation(query);
        for (int i = 0; i < ranges.length; ++i) {
            RangeFilterBuilder filter = FilterBuilders.rangeFilter("values");
            if (ranges[i][0] != Double.NEGATIVE_INFINITY) {
                filter = filter.from(ranges[i][0]);
            }
            if (ranges[i][1] != Double.POSITIVE_INFINITY){
                filter = filter.to(ranges[i][1]);
            }
            reqBuilder = reqBuilder.addAggregation(filter("filter" + i).filter(filter));
        }

        SearchResponse resp = reqBuilder.execute().actionGet();
        Range range = resp.getAggregations().get("range");

        for (int i = 0; i < ranges.length; ++i) {

            long count = 0;
            for (double[] values : docs) {
                for (double value : values) {
                    if (value >= ranges[i][0] && value < ranges[i][1]) {
                        ++count;
                        break;
                    }
                }
            }

            final Range.Bucket bucket = range.getBucketByKey(Integer.toString(i));
            assertEquals(bucket.getKey(), count, bucket.getDocCount());

            final Filter filter = resp.getAggregations().get("filter" + i);
            assertThat(filter.getDocCount(), equalTo(count));
        }
    }

    // test long/double/string terms aggs with high number of buckets that require array growth
    public void testDuelTerms() throws Exception {
        // These high numbers of docs and terms are important to trigger page recycling
        final int numDocs = atLeast(10000);
        final int maxNumTerms = randomIntBetween(10, 100000);

        final IntOpenHashSet valuesSet = new IntOpenHashSet();
        wipeIndices("idx");
        prepareCreate("idx").addMapping("type", jsonBuilder().startObject()
              .startObject("type")
                .startObject("properties")
                  .startObject("string_values")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                  .endObject()
                  .startObject("long_values")
                    .field("type", "long")
                  .endObject()
                  .startObject("double_values")
                    .field("type", "double")
                  .endObject()
                .endObject()
              .endObject()).execute().actionGet();
        for (int i = 0; i < numDocs; ++i) {
            final int[] values = new int[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomInt(maxNumTerms - 1) - 1000;
                valuesSet.add(values[j]);
            }
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("num", randomDouble())
                    .startArray("long_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(values[j]);
            }
            source = source.endArray().startArray("double_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value((double) values[j]);
            }
            source = source.endArray().startArray("string_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(Integer.toString(values[j]));
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx", "type").setSource(source).execute().actionGet();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenient()).execute().get());

        SearchResponse resp = client().prepareSearch("idx")
                .addAggregation(terms("long").field("long_values").size(maxNumTerms).subAggregation(min("min").field("num")))
                .addAggregation(terms("double").field("double_values").size(maxNumTerms).subAggregation(max("max").field("num")))
                .addAggregation(terms("string_map").field("string_values").executionHint(TermsAggregatorFactory.EXECUTION_HINT_VALUE_MAP).size(maxNumTerms).subAggregation(stats("stats").field("num")))
                .addAggregation(terms("string_ordinals").field("string_values").executionHint(TermsAggregatorFactory.EXECUTION_HINT_VALUE_ORDINALS).size(maxNumTerms).subAggregation(extendedStats("stats").field("num"))).execute().actionGet();
        assertEquals(0, resp.getFailedShards());

        final Terms longTerms = resp.getAggregations().get("long");
        final Terms doubleTerms = resp.getAggregations().get("double");
        final Terms stringMapTerms = resp.getAggregations().get("string_map");
        final Terms stringOrdinalsTerms = resp.getAggregations().get("string_ordinals");

        assertEquals(valuesSet.size(), longTerms.getBuckets().size());
        assertEquals(valuesSet.size(), doubleTerms.getBuckets().size());
        assertEquals(valuesSet.size(), stringMapTerms.getBuckets().size());
        assertEquals(valuesSet.size(), stringOrdinalsTerms.getBuckets().size());
        for (Terms.Bucket bucket : longTerms.getBuckets()) {
            final Terms.Bucket doubleBucket = doubleTerms.getBucketByKey(Double.toString(Long.parseLong(bucket.getKeyAsText().string())));
            final Terms.Bucket stringMapBucket = stringMapTerms.getBucketByKey(bucket.getKeyAsText().string());
            final Terms.Bucket stringOrdinalsBucket = stringOrdinalsTerms.getBucketByKey(bucket.getKeyAsText().string());
            assertNotNull(doubleBucket);
            assertNotNull(stringMapBucket);
            assertNotNull(stringOrdinalsBucket);
            assertEquals(bucket.getDocCount(), doubleBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringMapBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringOrdinalsBucket.getDocCount());
        }
    }

    // Duel between histograms and scripted terms
    public void testDuelTermsHistogram() throws Exception {
        createIndex("idx");

        final int numDocs = atLeast(1000);
        final int maxNumTerms = randomIntBetween(10, 2000);
        final int interval = randomIntBetween(1, 100);

        final Integer[] values = new Integer[maxNumTerms];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomInt(maxNumTerms * 3) - maxNumTerms;
        }

        for (int i = 0; i < numDocs; ++i) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("num", randomDouble())
                    .startArray("values");
            final int numValues = randomInt(4);
            for (int j = 0; j < numValues; ++j) {
                source = source.value(randomFrom(values));
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx", "type").setSource(source).execute().actionGet();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenient()).execute().get());

        SearchResponse resp = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("values").script("floor(_value / interval)").param("interval", interval).size(maxNumTerms))
                .addAggregation(histogram("histo").field("values").interval(interval))
                .execute().actionGet();

        assertThat(resp.getFailedShards(), equalTo(0));

        Terms terms = resp.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        Histogram histo = resp.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(terms.getBuckets().size(), equalTo(histo.getBuckets().size()));
        for (Terms.Bucket bucket : terms.getBuckets()) {
            final long key = bucket.getKeyAsNumber().longValue() * interval;
            final Histogram.Bucket histoBucket = histo.getBucketByKey(key);
            assertEquals(bucket.getDocCount(), histoBucket.getDocCount());
        }
    }

    public void testLargeNumbersOfPercentileBuckets() throws Exception {
        // test high numbers of percentile buckets to make sure paging and release work correctly
        createIndex("idx");

        final int numDocs = atLeast(25000);
        int t = 0;
        for (int i = 0; i < numDocs; ) {
            BulkRequestBuilder request = client().prepareBulk();
            final int bulkSize = Math.min(numDocs - i, 100);
            client().prepareIndex("idx", "type").setSource("double_value", randomDouble()).execute().actionGet();
            for (int j = 0; j < bulkSize; ++j) {
                ++t;
                request.add(client().prepareIndex("idx", "type", Integer.toString(i++)).setSource("double_value", randomDouble()));
            }
            BulkResponse response = request.execute().actionGet();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        assertEquals(numDocs, t);
        assertNoFailures(client().admin().indices().prepareRefresh("idx").execute().get());

        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("terms").field("double_value").subAggregation(percentiles("pcts").field("double_value"))).execute().actionGet();
        assertNoFailures(response);
    }

}
