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

package org.elasticsearch.search.aggregations.transformer;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.derivative;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class DerivativeTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";

    static int numDocs;
    static int interval;
    static int numValueBuckets, numValuesBuckets;
    static int numFirstDerivValueBuckets, numFirstDerivValuesBuckets;
    static long[] valueCounts, valuesCounts;
    static long[] firstDerivValueCounts, firstDerivValuesCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        numValueBuckets = numDocs / interval + 1;
        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket = (i + 1) / interval;
            valueCounts[bucket]++;
        }

        numValuesBuckets = (numDocs + 1) / interval + 1;
        valuesCounts = new long[numValuesBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket1 = (i + 1) / interval;
            final int bucket2 = (i + 2) / interval;
            valuesCounts[bucket1]++;
            if (bucket1 != bucket2) {
                valuesCounts[bucket2]++;
            }
        }

        numFirstDerivValueBuckets = numValueBuckets - 1;
        firstDerivValueCounts = new long[numFirstDerivValueBuckets];
        long lastValueCount = -1;
        for (int i = 0; i < numValueBuckets; i++) {
            long thisValue = valueCounts[i];
            if (lastValueCount != -1) {
                long diff = thisValue - lastValueCount;
                firstDerivValueCounts[i - 1] = diff;
            }
            lastValueCount = thisValue;
        }

        numFirstDerivValuesBuckets = numValuesBuckets - 1;
        firstDerivValuesCounts = new long[numFirstDerivValuesBuckets];
        long lastValuesCount = -1;
        for (int i = 0; i < numValuesBuckets; i++) {
            long thisValue = valuesCounts[i];
            if (lastValuesCount != -1) {
                long diff = thisValue - lastValuesCount;
                firstDerivValuesCounts[i - 1] = diff;
            }
            lastValuesCount = thisValue;
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i + 1).startArray(MULTI_VALUED_FIELD_NAME).value(i + 1)
                            .value(i + 2).endArray().field("tag", "tag" + i).endObject()));
        }

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void singleValuedField() {

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValueBuckets));

        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = deriv.getBucketByKey(i * interval);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        derivative("deriv").subAggregation(
                                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                        .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValueBuckets));
        Object[] propertiesKeys = (Object[]) deriv.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) deriv.getProperty("_count");
        Object[] propertiesDocCountDerivs = (Object[]) deriv.getProperty("_doc_count.value");
        Object[] propertiesCounts = (Object[]) deriv.getProperty("sum.value");

        List<Histogram.Bucket> buckets = new ArrayList<>(deriv.getBuckets());
        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
            SimpleValue sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long s1 = 0;
            long s2 = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == i) {
                    s1 += j + 1;
                }
                if ((j + 1) / interval == i + 1) {
                    s2 += j + 1;
                }
            }
            long s = s2 - s1;
            assertThat(sum.value(), equalTo((double) s));
            assertThat((String) propertiesKeys[i], equalTo(String.valueOf((long) i * interval)));
            assertThat((long) propertiesDocCounts[i], equalTo(0l));
            assertThat((double) propertiesDocCountDerivs[i], equalTo((double) firstDerivValueCounts[i]));
            assertThat((double) propertiesCounts[i], equalTo((double) s));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(MULTI_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValuesBuckets));

        for (int i = 0; i < numFirstDerivValuesBuckets; ++i) {
            Histogram.Bucket bucket = deriv.getBucketByKey(i * interval);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValuesCounts[i]));
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValueBuckets));

        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = deriv.getBucketByKey(i * interval);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
        }
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        derivative("deriv").subAggregation(
                                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1l).minDocCount(0)
                                        .subAggregation(histogram("sub_histo").interval(1l)))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));

        Histogram deriv = searchResponse.getAggregations().get("deriv");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(2));
        Histogram.Bucket bucket = deriv.getBucketByKey(0l);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));

        bucket = deriv.getBucketByKey(1l);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(1l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));
    }

}
