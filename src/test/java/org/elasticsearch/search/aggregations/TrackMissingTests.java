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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class TrackMissingTests extends ElasticsearchIntegrationTest {

    private static enum BucketAggBuilderFactory {

        TERMS() {
            @Override
            public ValuesSourceAggregationBuilder builder(String name) {
                return terms(name);
            }

            @Override
            public boolean numericOnly() {
                return false;
            }
        },
        RANGE() {
            @Override
            public ValuesSourceAggregationBuilder builder(String name) {
                return range(name).addRange(0, 10);
            }
        },
        HISTOGRAM() {
            @Override
            public ValuesSourceAggregationBuilder builder(String name) {
                return histogram(name).interval(1);
            }
        };

        public abstract ValuesSourceAggregationBuilder builder(String name);

        public boolean numericOnly() {
            return true;
        }
    }

    private static enum MetricsAggBuilderFactory {

        MIN() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return min(name);
            }
        },
        MAX() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return max(name);
            }
        },
        SUM() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return sum(name);
            }
        },
        AVG() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return avg(name);
            }
        },
        STATS() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return stats(name);
            }
        },
        EXTENDED_STATS() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return extendedStats(name);
            }
        },
        VALUE_COUNT() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return valueCount(name);
            }

            @Override
            public boolean numericOnly() {
                return false;
            }
        },
        CARDINALITY() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return cardinality(name);
            }

            @Override
            public boolean numericOnly() {
                return false;
            }
        },
        PERCENTILES() {
            @Override
            public ValuesSourceMetricsAggregationBuilder builder(String name) {
                return percentiles(name);
            }
        };

        public abstract ValuesSourceMetricsAggregationBuilder builder(String name);

        public boolean numericOnly() {
            return true;
        }
    }

    private final String[] fields = { "lvalue", "dvalue", "svalue" };

    int missingCount;
    int existCount;
    int docCount;


    @Before
    public void init() throws Exception {

        existCount = randomIntBetween(1, 10);
        missingCount = randomIntBetween(1, 10);
        docCount = missingCount + existCount;

        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[docCount];
        for (int i = 0; i < existCount; i++) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .field("svalue", "val" + i)
                    .field("lvalue", i)
                    .field("dvalue", (double) i)
                    .endObject());
        }
        for (int i = 0; i < missingCount; i++) {
            builders[existCount + i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    private BucketAggBuilderFactory randomBucketBuilderFactory() {
        int i = randomIntBetween(0, BucketAggBuilderFactory.values().length - 1);
        return BucketAggBuilderFactory.values()[i];
    }

    private MetricsAggBuilderFactory randomMetricsBuilderFactory() {
        int i = randomIntBetween(0, MetricsAggBuilderFactory.values().length - 1);
        return MetricsAggBuilderFactory.values()[i];
    }

    @Test
    public void bucket() throws Exception {

        BucketAggBuilderFactory factory = randomBucketBuilderFactory();
        String field;
        if (factory.numericOnly()) {
            field = randomBoolean() ? "lvalue" : "dvalue";
        } else {
            field = fields[randomIntBetween(0, fields.length - 1)];
        }

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(factory.builder("agg").field(field).trackMissing(true))
                .execute().actionGet();

        assertSearchResponse(response);

        MultiBucketsAggregation agg = response.getAggregations().get("agg");
        assertThat(agg, notNullValue());
        assertThat(agg.getName(), equalTo("agg"));

        Missing missing = response.getAggregations().get("agg_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo((long) missingCount));
    }

    @Test
    public void bucketWithName() throws Exception {

        BucketAggBuilderFactory factory = randomBucketBuilderFactory();
        String field;
        if (factory.numericOnly()) {
            field = randomBoolean() ? "lvalue" : "dvalue";
        } else {
            field = fields[randomIntBetween(0, fields.length - 1)];
        }

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(factory.builder("agg").field(field).trackMissing("_missing"))
                .execute().actionGet();

        assertSearchResponse(response);

        MultiBucketsAggregation agg = response.getAggregations().get("agg");
        assertThat(agg, notNullValue());
        assertThat(agg.getName(), equalTo("agg"));

        Missing missing = response.getAggregations().get("_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo((long) missingCount));
    }

    @Test
    public void metrics() throws Exception {

        MetricsAggBuilderFactory factory = randomMetricsBuilderFactory();
        String field;
        if (factory.numericOnly()) {
            field = randomBoolean() ? "lvalue" : "dvalue";
        } else {
            field = fields[randomIntBetween(0, fields.length - 1)];
        }

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(factory.builder("agg").field(field).trackMissing(true))
                .execute().actionGet();

        assertSearchResponse(response);

        Aggregation agg = response.getAggregations().get("agg");
        assertThat(agg, notNullValue());
        assertThat(agg.getName(), equalTo("agg"));

        Missing missing = response.getAggregations().get("agg_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo((long) missingCount));
    }

    @Test
    public void metricsWithName() throws Exception {

        MetricsAggBuilderFactory factory = randomMetricsBuilderFactory();
        String field;
        if (factory.numericOnly()) {
            field = randomBoolean() ? "lvalue" : "dvalue";
        } else {
            field = fields[randomIntBetween(0, fields.length - 1)];
        }

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(factory.builder("agg").field(field).trackMissing("_missing"))
                .execute().actionGet();

        assertSearchResponse(response);

        Aggregation agg = response.getAggregations().get("agg");
        assertThat(agg, notNullValue());
        assertThat(agg.getName(), equalTo("agg"));

        Missing missing = response.getAggregations().get("_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo((long) missingCount));
    }



    @Test
    public void withInheritance() throws Exception {
        String field = randomBoolean() ? "lvalue" : "dvalue";
        AggregationBuilder topAggBuilder = randomBucketBuilderFactory().builder("topAgg").field(field).trackMissing(true);
        AbstractAggregationBuilder subAggBuilder = randomBoolean() ?
                randomBucketBuilderFactory().builder("subAgg").trackMissing(true) :
                randomMetricsBuilderFactory().builder("subAgg").trackMissing(true);

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(topAggBuilder.subAggregation(subAggBuilder))
                .execute().actionGet();

        assertSearchResponse(response);

        Missing missing = response.getAggregations().get("topAgg_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo((long) missingCount));

        MultiBucketsAggregation topAgg = response.getAggregations().get("topAgg");
        assertThat(topAgg, notNullValue());

        MultiBucketsAggregation.Bucket bucket = topAgg.getBuckets().iterator().next();
        assertThat(bucket, notNullValue());

        Aggregation subAgg = bucket.getAggregations().get("subAgg");
        assertThat(subAgg, notNullValue());
        assertThat(subAgg.getName(), equalTo("subAgg"));

        missing = bucket.getAggregations().get("subAgg_missing");
        assertThat(missing, notNullValue());
        assertThat(missing.getDocCount(), equalTo(0l));
    }

}