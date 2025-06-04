/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class FilterIT extends ESIntegTestCase {

    static int numDocs, numTag1Docs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            builders.add(
                prepareIndex("idx").setId("" + i)
                    .setSource(jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject())
            );
        }
        for (int i = numTag1Docs; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .field("value", i)
                .field("tag", "tag2")
                .field("name", "name" + i)
                .endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted docs that match the filter
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();
        for (int i = 0; i < 2; i++) {
            builders.add(
                prepareIndex("empty_bucket_idx").setId("" + i).setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        assertNoFailuresAndResponse(prepareSearch("idx").addAggregation(filter("tag1", termQuery("tag", "tag1"))), response -> {
            SingleBucketAggregation filter = response.getAggregations().get("tag1");
            assertThat(filter, notNullValue());
            assertThat(filter.getName(), equalTo("tag1"));
            assertThat(filter.getDocCount(), equalTo((long) numTag1Docs));
        });
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        assertNoFailuresAndResponse(prepareSearch("idx").addAggregation(filter("tag1", emptyFilter)), response -> {
            SingleBucketAggregation filter = response.getAggregations().get("tag1");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo((long) numDocs));
        });
    }

    public void testWithSubAggregation() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(filter("tag1", termQuery("tag", "tag1")).subAggregation(avg("avg_value").field("value"))),
            response -> {
                SingleBucketAggregation filter = response.getAggregations().get("tag1");
                assertThat(filter, notNullValue());
                assertThat(filter.getName(), equalTo("tag1"));
                assertThat(filter.getDocCount(), equalTo((long) numTag1Docs));
                assertThat((long) ((InternalAggregation) filter).getProperty("_count"), equalTo((long) numTag1Docs));

                long sum = 0;
                for (int i = 0; i < numTag1Docs; ++i) {
                    sum += i + 1;
                }
                assertThat(filter.getAggregations().asList().isEmpty(), is(false));
                Avg avgValue = filter.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
                assertThat((double) ((InternalAggregation) filter).getProperty("avg_value.value"), equalTo((double) sum / numTag1Docs));
            }
        );
    }

    public void testAsSubAggregation() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field("value").interval(2L).subAggregation(filter("filter", matchAllQuery()))
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

                for (Histogram.Bucket bucket : histo.getBuckets()) {
                    SingleBucketAggregation filter = bucket.getAggregations().get("filter");
                    assertThat(filter, notNullValue());
                    assertEquals(bucket.getDocCount(), filter.getDocCount());
                }
            }
        );
    }

    public void testWithContextBasedSubAggregation() throws Exception {
        try {
            prepareSearch("idx").addAggregation(filter("tag1", termQuery("tag", "tag1")).subAggregation(avg("avg_value"))).get();

            fail(
                "expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source"
                    + "context which the sub-aggregation can inherit"
            );

        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testEmptyAggregation() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("empty_bucket_idx").setQuery(matchAllQuery())
                .addAggregation(
                    histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(filter("filter", matchAllQuery()))
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, Matchers.notNullValue());
                Histogram.Bucket bucket = histo.getBuckets().get(1);
                assertThat(bucket, Matchers.notNullValue());

                SingleBucketAggregation filter = bucket.getAggregations().get("filter");
                assertThat(filter, Matchers.notNullValue());
                assertThat(filter.getName(), equalTo("filter"));
                assertThat(filter.getDocCount(), is(0L));
            }
        );
    }
}
