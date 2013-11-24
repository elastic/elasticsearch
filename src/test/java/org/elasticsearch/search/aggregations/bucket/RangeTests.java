/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class RangeTests extends ElasticsearchIntegrationTest {


    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    int numDocs;

    @Before
    public void init() throws Exception {
        createIndex("idx");
        numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+1).value(i+2).endArray()
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");

    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo("r1", 3)
                        .addRange("r2", 3, 6)
                        .addUnboundedFrom("r3", 6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(3.0)); // 1 + 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(12.0)); // 3 + 4 + 5

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        long total = 0;
        for (int i = 5; i < numDocs; ++i) {
            total += i + 1;
        }
        assertThat(sum.getValue(), equalTo((double) total));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(avg("avg")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(1.5)); // (1 + 2) / 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(4.0)); // (3 + 4 + 5) / 3

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        long total = 0;
        for (int i = 5; i < numDocs; ++i) {
            total += i + 1;
        }
        assertThat(avg.getValue(), equalTo((double) total / (numDocs - 5))); // (6 + 7 + 8 + 9 + 10) / 5
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l)); // 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l)); // 3, 4, 5

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]
     */

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 3l));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]

    r1: 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12
     */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(2d+3d));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2+3+3+4+4+5+5+6));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 3L));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        long total = 0;
        for (int i = 3; i < numDocs; ++i) {
            total += ((i + 1) + 1) + ((i + 1) + 2);
        }
        assertThat(sum.getValue(), equalTo((double) total));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['value'].value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['value'].value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(avg("avg")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(1.5)); // (1 + 2) / 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(4.0)); // (3 + 4 + 5) / 3

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        long total = 0;
        for (int i = 5; i < numDocs; ++i) {
            total += i + 1;
        }
        assertThat(avg.getValue(), equalTo((double) total / (numDocs - 5))); // (6 + 7 + 8 + 9 + 10) / 5
    }

    @Test
    public void emptyRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .addUnboundedTo(-1)
                        .addUnboundedFrom(1000))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(2));

        Range.Bucket bucket = range.getByKey("*--1.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*--1.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(-1.0));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("1000.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("1000.0-*"));
        assertThat(bucket.getFrom(), equalTo(1000d));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['values'].values")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }
    
    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    
    r1: 1, 2, 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11
     */

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['values'].values")
                        .addUnboundedTo("r1", 3)
                        .addRange("r2", 3, 6)
                        .addUnboundedFrom("r3", 6)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+2+3));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2+3+3+4+4+5+5+6));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        long total = 0;
        for (int i = 4; i < numDocs; ++i) {
            total += (i + 1) + (i + 2);
        }
        assertThat(sum.getValue(), equalTo((double) total));
    }

    @Test
    public void unmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5l));
    }

    @Test
    public void overlappingRanges() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .addUnboundedTo(5)
                        .addRange(3, 6)
                        .addRange(4, 5)
                        .addUnboundedFrom(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(4));

        Range.Bucket bucket = range.getByKey("*-5.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-5.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(5.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("4.0-5.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("4.0-5.0"));
        assertThat(bucket.getFrom(), equalTo(4.0));
        assertThat(bucket.getTo(), equalTo(5.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("4.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("4.0-*"));
        assertThat(bucket.getFrom(), equalTo(4.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i * 2)
                    .endObject()));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).emptyBuckets(true)
                        .subAggregation(range("range").addRange("0-2", 0.0, 2.0)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        Range range = bucket.getAggregations().get("range");
        assertThat(range, Matchers.notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), is(1));
        assertThat(range.buckets().get(0).getKey(), equalTo("0-2"));
        assertThat(range.buckets().get(0).getFrom(), equalTo(0.0));
        assertThat(range.buckets().get(0).getTo(), equalTo(2.0));
        assertThat(range.buckets().get(0).getDocCount(), equalTo(0l));

    }
}
