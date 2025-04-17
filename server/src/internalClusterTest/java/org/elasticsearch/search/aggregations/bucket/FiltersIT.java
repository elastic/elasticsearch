/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class FiltersIT extends ESIntegTestCase {

    static int numDocs, numTag1Docs, numTag2Docs, numOtherDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        numTag2Docs = randomIntBetween(1, numDocs - numTag1Docs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted docs that match the filter
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .field("value", i)
                .field("tag", "tag2")
                .field("name", "name" + i)
                .endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs + numTag2Docs; i < numDocs; i++) {
            numOtherDocs++;
            XContentBuilder source = jsonBuilder().startObject()
                .field("value", i)
                .field("tag", "tag3")
                .field("name", "name" + i)
                .endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
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

    // This test replicates a strange filter query & filters aggregation behavior
    // we apparently utilize competitive iterators strangely.
    // See: https://github.com/elastic/elasticsearch/issues/126955
    public void testSimpleWithFilterQuery() throws Exception {
        createIndex("filters_idx");
        String groupFieldName = "group";
        String subGroupFieldName = "subGroup";

        int numTotalGroup0 = 500;
        String group0Name = "group0";

        int numTotalGroup1 = 1000;
        String group1Name = "group1";

        int subGroup0 = 100;
        String subGroup0Name = "subGroup0";

        int subGroup1 = 50;
        String subGroup1Name = "subGroup1";

        int subGroup2 = 25;
        String subGroup2Name = "subGroup2";
        int others = 10;
        String otherName = "others";
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTotalGroup0; i++) {
            for (int j = 0; j < subGroup0; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group0Name)
                    .field(subGroupFieldName, subGroup0Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < subGroup1; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group0Name)
                    .field(subGroupFieldName, subGroup1Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < subGroup2; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group0Name)
                    .field(subGroupFieldName, subGroup2Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < others; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group0Name)
                    .field(subGroupFieldName, otherName)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
        }
        for (int i = 0; i < numTotalGroup1; i++) {
            for (int j = 0; j < subGroup0; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group1Name)
                    .field(subGroupFieldName, subGroup0Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < subGroup1; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group1Name)
                    .field(subGroupFieldName, subGroup1Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < subGroup2; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group1Name)
                    .field(subGroupFieldName, subGroup2Name)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
            for (int j = 0; j < others; j++) {
                XContentBuilder source = jsonBuilder().startObject()
                    .field(groupFieldName, group1Name)
                    .field(subGroupFieldName, otherName)
                    .endObject();
                builders.add(prepareIndex("filters_idx").setSource(source));
            }
        }
        indexRandom(true, false, true, builders);
        ensureSearchable();
        assertNoFailuresAndResponse(
            prepareSearch("filters_idx").setSize(0)
                .setRequestCache(false)
                .setTrackTotalHits(true)
                .setQuery(boolQuery().filter(termQuery(groupFieldName + ".keyword", group0Name)))
                .addAggregation(
                    filters(
                        "results",
                        new KeyedFilter(subGroup0Name, termsQuery(subGroupFieldName + ".keyword", subGroup0Name)),
                        new KeyedFilter(subGroup1Name, termsQuery(subGroupFieldName + ".keyword", subGroup1Name)),
                        new KeyedFilter(subGroup2Name, termsQuery(subGroupFieldName + ".keyword", subGroup2Name))
                        // This is key
                    ).otherBucket(false)
                ),
            searchResponse -> {
                Filters filters = searchResponse.getAggregations().get("results");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("results"));
                Filters.Bucket bucket = filters.getBucketByKey(subGroup0Name);
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) subGroup0 * numTotalGroup0));
            }
        );
    }

    public void testSimple() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                )
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(2));

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = filters.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
            }
        );
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters("tags", randomOrder(new KeyedFilter("all", emptyFilter), new KeyedFilter("tag1", termQuery("tag", "tag1"))))
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                Filters.Bucket allBucket = filters.getBucketByKey("all");
                assertThat(allBucket.getDocCount(), equalTo((long) numDocs));

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
            }
        );
    }

    public void testWithSubAggregation() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).subAggregation(avg("avg_value").field("value"))
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(2));
                assertThat(((InternalAggregation) filters).getProperty("_bucket_count"), equalTo(2));
                Object[] propertiesKeys = (Object[]) ((InternalAggregation) filters).getProperty("_key");
                Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) filters).getProperty("_count");
                Object[] propertiesCounts = (Object[]) ((InternalAggregation) filters).getProperty("avg_value.value");

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
                long sum = 0;
                for (int i = 0; i < numTag1Docs; ++i) {
                    sum += i + 1;
                }
                assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
                Avg avgValue = bucket.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
                assertThat((String) propertiesKeys[0], equalTo("tag1"));
                assertThat((long) propertiesDocCounts[0], equalTo((long) numTag1Docs));
                assertThat((double) propertiesCounts[0], equalTo((double) sum / numTag1Docs));

                bucket = filters.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
                sum = 0;
                for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); ++i) {
                    sum += i;
                }
                assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
                avgValue = bucket.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numTag2Docs));
                assertThat(propertiesKeys[1], equalTo("tag2"));
                assertThat(propertiesDocCounts[1], equalTo((long) numTag2Docs));
                assertThat(propertiesCounts[1], equalTo((double) sum / numTag2Docs));
            }
        );
    }

    public void testAsSubAggregation() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field("value").interval(2L).subAggregation(filters("filters", matchAllQuery()))
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

                for (Histogram.Bucket bucket : histo.getBuckets()) {
                    Filters filters = bucket.getAggregations().get("filters");
                    assertThat(filters, notNullValue());
                    assertThat(filters.getBuckets().size(), equalTo(1));
                    Filters.Bucket filterBucket = filters.getBuckets().get(0);
                    assertEquals(bucket.getDocCount(), filterBucket.getDocCount());
                }
            }
        );
    }

    public void testWithContextBasedSubAggregation() throws Exception {

        try {
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).subAggregation(avg("avg_value"))
            ).get();

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
                    histogram("histo").field("value")
                        .interval(1L)
                        .minDocCount(0)
                        .subAggregation(filters("filters", new KeyedFilter("all", matchAllQuery())))
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, Matchers.notNullValue());
                Histogram.Bucket bucket = histo.getBuckets().get(1);
                assertThat(bucket, Matchers.notNullValue());

                Filters filters = bucket.getAggregations().get("filters");
                assertThat(filters, notNullValue());
                Filters.Bucket all = filters.getBucketByKey("all");
                assertThat(all, Matchers.notNullValue());
                assertThat(all.getKeyAsString(), equalTo("all"));
                assertThat(all.getDocCount(), is(0L));
            }
        );
    }

    public void testSimpleNonKeyed() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(filters("tags", termQuery("tag", "tag1"), termQuery("tag", "tag2"))),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(2));

                Collection<? extends Filters.Bucket> buckets = filters.getBuckets();
                Iterator<? extends Filters.Bucket> itr = buckets.iterator();

                Filters.Bucket bucket = itr.next();
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = itr.next();
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
            }
        );
    }

    public void testOtherBucket() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true)
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(3));

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = filters.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

                bucket = filters.getBucketByKey("_other_");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
            }
        );
    }

    public void testOtherNamedBucket() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true).otherBucketKey("foobar")
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(3));

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = filters.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

                bucket = filters.getBucketByKey("foobar");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
            }
        );
    }

    public void testOtherNonKeyed() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(filters("tags", termQuery("tag", "tag1"), termQuery("tag", "tag2")).otherBucket(true)),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(3));

                Collection<? extends Filters.Bucket> buckets = filters.getBuckets();
                Iterator<? extends Filters.Bucket> itr = buckets.iterator();

                Filters.Bucket bucket = itr.next();
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = itr.next();
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

                bucket = itr.next();
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
            }
        );
    }

    public void testOtherWithSubAggregation() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true).subAggregation(avg("avg_value").field("value"))
            ),
            response -> {
                Filters filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                assertThat(filters.getName(), equalTo("tags"));

                assertThat(filters.getBuckets().size(), equalTo(3));
                assertThat(((InternalAggregation) filters).getProperty("_bucket_count"), equalTo(3));
                Object[] propertiesKeys = (Object[]) ((InternalAggregation) filters).getProperty("_key");
                Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) filters).getProperty("_count");
                Object[] propertiesCounts = (Object[]) ((InternalAggregation) filters).getProperty("avg_value.value");

                Filters.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
                long sum = 0;
                for (int i = 0; i < numTag1Docs; ++i) {
                    sum += i + 1;
                }
                assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
                Avg avgValue = bucket.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
                assertThat(propertiesKeys[0], equalTo("tag1"));
                assertThat(propertiesDocCounts[0], equalTo((long) numTag1Docs));
                assertThat(propertiesCounts[0], equalTo((double) sum / numTag1Docs));

                bucket = filters.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
                sum = 0;
                for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); ++i) {
                    sum += i;
                }
                assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
                avgValue = bucket.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numTag2Docs));
                assertThat(propertiesKeys[1], equalTo("tag2"));
                assertThat(propertiesDocCounts[1], equalTo((long) numTag2Docs));
                assertThat(propertiesCounts[1], equalTo((double) sum / numTag2Docs));

                bucket = filters.getBucketByKey("_other_");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
                sum = 0;
                for (int i = numTag1Docs + numTag2Docs; i < numDocs; ++i) {
                    sum += i;
                }
                assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
                avgValue = bucket.getAggregations().get("avg_value");
                assertThat(avgValue, notNullValue());
                assertThat(avgValue.getName(), equalTo("avg_value"));
                assertThat(avgValue.getValue(), equalTo((double) sum / numOtherDocs));
                assertThat(propertiesKeys[2], equalTo("_other_"));
                assertThat(propertiesDocCounts[2], equalTo((long) numOtherDocs));
                assertThat(propertiesCounts[2], equalTo((double) sum / numOtherDocs));
            }
        );
    }

    public void testEmptyAggregationWithOtherBucket() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("empty_bucket_idx").setQuery(matchAllQuery())
                .addAggregation(
                    histogram("histo").field("value")
                        .interval(1L)
                        .minDocCount(0)
                        .subAggregation(filters("filters", new KeyedFilter("foo", matchAllQuery())).otherBucket(true).otherBucketKey("bar"))
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, Matchers.notNullValue());
                Histogram.Bucket bucket = histo.getBuckets().get(1);
                assertThat(bucket, Matchers.notNullValue());

                Filters filters = bucket.getAggregations().get("filters");
                assertThat(filters, notNullValue());

                Filters.Bucket other = filters.getBucketByKey("bar");
                assertThat(other, Matchers.notNullValue());
                assertThat(other.getKeyAsString(), equalTo("bar"));
                assertThat(other.getDocCount(), is(0L));
            }
        );
    }

    private static KeyedFilter[] randomOrder(KeyedFilter... filters) {
        Collections.shuffle(Arrays.asList(filters), random());
        return filters;
    }
}
