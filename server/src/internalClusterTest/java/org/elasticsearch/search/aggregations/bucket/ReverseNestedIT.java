/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.reverseNested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class ReverseNestedIT extends ESIntegTestCase {

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate("idx1").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("alias")
                    .field("type", "alias")
                    .field("path", "field1")
                    .endObject()
                    .startObject("nested1")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("field2")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        assertAcked(
            prepareCreate("idx2").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("nested2")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("field2")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        insertIdx1(Arrays.asList("a", "b", "c"), Arrays.asList("1", "2", "3", "4"));
        insertIdx1(Arrays.asList("b", "c", "d"), Arrays.asList("4", "5", "6", "7"));
        insertIdx1(Arrays.asList("c", "d", "e"), Arrays.asList("7", "8", "9", "1"));
        refresh();
        insertIdx1(Arrays.asList("a", "e"), Arrays.asList("7", "4", "1", "1"));
        insertIdx1(Arrays.asList("a", "c"), Arrays.asList("2", "1"));
        insertIdx1(Arrays.asList("a"), Arrays.asList("3", "4"));
        refresh();
        insertIdx1(Arrays.asList("x", "c"), Arrays.asList("1", "8"));
        insertIdx1(Arrays.asList("y", "c"), Arrays.asList("6"));
        insertIdx1(Arrays.asList("z"), Arrays.asList("5", "9"));
        refresh();

        insertIdx2(
            new String[][] { new String[] { "a", "0", "0", "1", "2" }, new String[] { "b", "0", "1", "1", "2" }, new String[] { "a", "0" } }
        );
        insertIdx2(new String[][] { new String[] { "c", "1", "1", "2", "2" }, new String[] { "d", "3", "4" } });
        refresh();

        insertIdx2(new String[][] { new String[] { "a", "0", "0", "0", "0" }, new String[] { "b", "0", "0", "0", "0" } });
        insertIdx2(new String[][] { new String[] { "e", "1", "2" }, new String[] { "f", "3", "4" } });
        refresh();

        ensureSearchable();
    }

    private void insertIdx1(List<String> values1, List<String> values2) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().array("field1", values1.toArray()).startArray("nested1");
        for (String value1 : values2) {
            source.startObject().field("field2", value1).endObject();
        }
        source.endArray().endObject();
        indexRandom(false, prepareIndex("idx1").setRouting("1").setSource(source));
    }

    private void insertIdx2(String[][] values) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().startArray("nested1");
        for (String[] value : values) {
            source.startObject().field("field1", value[0]).startArray("nested2");
            for (int i = 1; i < value.length; i++) {
                source.startObject().field("field2", value[i]).endObject();
            }
            source.endArray().endObject();
        }
        source.endArray().endObject();
        indexRandom(false, prepareIndex("idx2").setRouting("1").setSource(source));
    }

    public void testSimpleReverseNestedToRoot() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx1").addAggregation(
                nested("nested1", "nested1").subAggregation(
                    terms("field2").field("nested1.field2")
                        .subAggregation(
                            reverseNested("nested1_to_field1").subAggregation(
                                terms("field1").field("field1").collectMode(randomFrom(SubAggCollectionMode.values()))
                            )
                        )
                )
            ),
            response -> {
                SingleBucketAggregation nested = response.getAggregations().get("nested1");
                assertThat(nested, notNullValue());
                assertThat(nested.getName(), equalTo("nested1"));
                assertThat(nested.getDocCount(), equalTo(25L));
                assertThat(nested.getAggregations().asList().isEmpty(), is(false));

                Terms usernames = nested.getAggregations().get("field2");
                assertThat(usernames, notNullValue());
                assertThat(usernames.getBuckets().size(), equalTo(9));
                List<Terms.Bucket> usernameBuckets = new ArrayList<>(usernames.getBuckets());

                // nested.field2: 1
                Terms.Bucket bucket = usernameBuckets.get(0);
                assertThat(bucket.getKeyAsString(), equalTo("1"));
                assertThat(bucket.getDocCount(), equalTo(6L));
                SingleBucketAggregation reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(((InternalAggregation) reverseNested).getProperty("_count"), equalTo(5L));
                Terms tags = reverseNested.getAggregations().get("field1");
                assertThat(((InternalAggregation) reverseNested).getProperty("field1"), sameInstance(tags));
                List<Terms.Bucket> tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(6));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(4L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(3L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(5).getKeyAsString(), equalTo("x"));
                assertThat(tagsBuckets.get(5).getDocCount(), equalTo(1L));

                // nested.field2: 4
                bucket = usernameBuckets.get(1);
                assertThat(bucket.getKeyAsString(), equalTo("4"));
                assertThat(bucket.getDocCount(), equalTo(4L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(5));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(3L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));

                // nested.field2: 7
                bucket = usernameBuckets.get(2);
                assertThat(bucket.getKeyAsString(), equalTo("7"));
                assertThat(bucket.getDocCount(), equalTo(3L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(5));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));

                // nested.field2: 2
                bucket = usernameBuckets.get(3);
                assertThat(bucket.getKeyAsString(), equalTo("2"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(3));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));

                // nested.field2: 3
                bucket = usernameBuckets.get(4);
                assertThat(bucket.getKeyAsString(), equalTo("3"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(3));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));

                // nested.field2: 5
                bucket = usernameBuckets.get(5);
                assertThat(bucket.getKeyAsString(), equalTo("5"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("z"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

                // nested.field2: 6
                bucket = usernameBuckets.get(6);
                assertThat(bucket.getKeyAsString(), equalTo("6"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("y"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

                // nested.field2: 8
                bucket = usernameBuckets.get(7);
                assertThat(bucket.getKeyAsString(), equalTo("8"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("x"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

                // nested.field2: 9
                bucket = usernameBuckets.get(8);
                assertThat(bucket.getKeyAsString(), equalTo("9"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("z"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testSimpleNested1ToRootToNested2() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx2").addAggregation(
                nested("nested1", "nested1").subAggregation(
                    reverseNested("nested1_to_root").subAggregation(nested("root_to_nested2", "nested1.nested2"))
                )
            ),
            response -> {
                SingleBucketAggregation nested = response.getAggregations().get("nested1");
                assertThat(nested.getName(), equalTo("nested1"));
                assertThat(nested.getDocCount(), equalTo(9L));
                SingleBucketAggregation reverseNested = nested.getAggregations().get("nested1_to_root");
                assertThat(reverseNested.getName(), equalTo("nested1_to_root"));
                assertThat(reverseNested.getDocCount(), equalTo(4L));
                nested = reverseNested.getAggregations().get("root_to_nested2");
                assertThat(nested.getName(), equalTo("root_to_nested2"));
                assertThat(nested.getDocCount(), equalTo(27L));
            }
        );
    }

    public void testSimpleReverseNestedToNested1() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx2").addAggregation(
                nested("nested1", "nested1.nested2").subAggregation(
                    terms("field2").field("nested1.nested2.field2")
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .size(10000)
                        .subAggregation(
                            reverseNested("nested1_to_field1").path("nested1")
                                .subAggregation(
                                    terms("field1").field("nested1.field1")
                                        .order(BucketOrder.key(true))
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                )
                        )
                )
            ),
            response -> {
                SingleBucketAggregation nested = response.getAggregations().get("nested1");
                assertThat(nested, notNullValue());
                assertThat(nested.getName(), equalTo("nested1"));
                assertThat(nested.getDocCount(), equalTo(27L));
                assertThat(nested.getAggregations().asList().isEmpty(), is(false));

                Terms usernames = nested.getAggregations().get("field2");
                assertThat(usernames, notNullValue());
                assertThat(usernames.getBuckets().size(), equalTo(5));
                List<Terms.Bucket> usernameBuckets = new ArrayList<>(usernames.getBuckets());

                Terms.Bucket bucket = usernameBuckets.get(0);
                assertThat(bucket.getKeyAsString(), equalTo("0"));
                assertThat(bucket.getDocCount(), equalTo(12L));
                SingleBucketAggregation reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(reverseNested.getDocCount(), equalTo(5L));
                Terms tags = reverseNested.getAggregations().get("field1");
                List<Terms.Bucket> tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(2));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(3L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));

                bucket = usernameBuckets.get(1);
                assertThat(bucket.getKeyAsString(), equalTo("1"));
                assertThat(bucket.getDocCount(), equalTo(6L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(reverseNested.getDocCount(), equalTo(4L));
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

                bucket = usernameBuckets.get(2);
                assertThat(bucket.getKeyAsString(), equalTo("2"));
                assertThat(bucket.getDocCount(), equalTo(5L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(reverseNested.getDocCount(), equalTo(4L));
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(4));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
                assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
                assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("e"));
                assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

                bucket = usernameBuckets.get(3);
                assertThat(bucket.getKeyAsString(), equalTo("3"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(reverseNested.getDocCount(), equalTo(2L));
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(2));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("f"));

                bucket = usernameBuckets.get(4);
                assertThat(bucket.getKeyAsString(), equalTo("4"));
                assertThat(bucket.getDocCount(), equalTo(2L));
                reverseNested = bucket.getAggregations().get("nested1_to_field1");
                assertThat(reverseNested.getDocCount(), equalTo(2L));
                tags = reverseNested.getAggregations().get("field1");
                tagsBuckets = new ArrayList<>(tags.getBuckets());
                assertThat(tagsBuckets.size(), equalTo(2));
                assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("d"));
                assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
                assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("f"));
            }
        );
    }

    public void testReverseNestedAggWithoutNestedAgg() {
        try {
            prepareSearch("idx2").addAggregation(
                terms("field2").field("nested1.nested2.field2")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(
                        reverseNested("nested1_to_field1").subAggregation(
                            terms("field1").field("nested1.field1").collectMode(randomFrom(SubAggCollectionMode.values()))
                        )
                    )
            ).get();
            fail("Expected SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testNonExistingNestedField() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx2").setQuery(matchAllQuery())
                .addAggregation(nested("nested2", "nested1.nested2").subAggregation(reverseNested("incorrect").path("nested3"))),
            response -> {

                SingleBucketAggregation nested = response.getAggregations().get("nested2");
                assertThat(nested, notNullValue());
                assertThat(nested.getName(), equalTo("nested2"));

                SingleBucketAggregation reverseNested = nested.getAggregations().get("incorrect");
                assertThat(reverseNested.getDocCount(), is(0L));
            }
        );

        // Test that parsing the reverse_nested agg doesn't fail, because the parent nested agg is unmapped:
        assertNoFailuresAndResponse(
            prepareSearch("idx1").setQuery(matchAllQuery())
                .addAggregation(nested("incorrect1", "incorrect1").subAggregation(reverseNested("incorrect2").path("incorrect2"))),
            response -> {

                SingleBucketAggregation nested = response.getAggregations().get("incorrect1");
                assertThat(nested, notNullValue());
                assertThat(nested.getName(), equalTo("incorrect1"));
                assertThat(nested.getDocCount(), is(0L));
            }
        );
    }

    public void testSameParentDocHavingMultipleBuckets() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("id")
            .field("type", "long")
            .endObject()
            .startObject("category")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject("sku")
            .field("type", "nested")
            .startObject("properties")
            .startObject("sku_type")
            .field("type", "keyword")
            .endObject()
            .startObject("colors")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("idx3").setSettings(indexSettings(1, 0)).setMapping(mapping));

        prepareIndex("idx3").setId("1")
            .setRefreshPolicy(IMMEDIATE)
            .setSource(
                jsonBuilder().startObject()
                    .startArray("sku")
                    .startObject()
                    .field("sku_type", "bar1")
                    .startArray("colors")
                    .startObject()
                    .field("name", "red")
                    .endObject()
                    .startObject()
                    .field("name", "green")
                    .endObject()
                    .startObject()
                    .field("name", "yellow")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("sku_type", "bar1")
                    .startArray("colors")
                    .startObject()
                    .field("name", "red")
                    .endObject()
                    .startObject()
                    .field("name", "blue")
                    .endObject()
                    .startObject()
                    .field("name", "white")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("sku_type", "bar1")
                    .startArray("colors")
                    .startObject()
                    .field("name", "black")
                    .endObject()
                    .startObject()
                    .field("name", "blue")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("sku_type", "bar2")
                    .startArray("colors")
                    .startObject()
                    .field("name", "orange")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("sku_type", "bar2")
                    .startArray("colors")
                    .startObject()
                    .field("name", "pink")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .startArray("category")
                    .startObject()
                    .field("name", "abc")
                    .endObject()
                    .startObject()
                    .field("name", "klm")
                    .endObject()
                    .startObject()
                    .field("name", "xyz")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        assertNoFailuresAndResponse(
            prepareSearch("idx3").addAggregation(
                nested("nested_0", "category").subAggregation(
                    terms("group_by_category").field("category.name")
                        .subAggregation(
                            reverseNested("to_root").subAggregation(
                                nested("nested_1", "sku").subAggregation(
                                    filter("filter_by_sku", termQuery("sku.sku_type", "bar1")).subAggregation(
                                        count("sku_count").field("sku.sku_type")
                                    )
                                )
                            )
                        )
                )
            ),
            response -> {
                assertHitCount(response, 1);

                SingleBucketAggregation nested0 = response.getAggregations().get("nested_0");
                assertThat(nested0.getDocCount(), equalTo(3L));
                Terms terms = nested0.getAggregations().get("group_by_category");
                assertThat(terms.getBuckets().size(), equalTo(3));
                for (String bucketName : new String[] { "abc", "klm", "xyz" }) {
                    logger.info("Checking results for bucket {}", bucketName);
                    Terms.Bucket bucket = terms.getBucketByKey(bucketName);
                    assertThat(bucket.getDocCount(), equalTo(1L));
                    SingleBucketAggregation toRoot = bucket.getAggregations().get("to_root");
                    assertThat(toRoot.getDocCount(), equalTo(1L));
                    SingleBucketAggregation nested1 = toRoot.getAggregations().get("nested_1");
                    assertThat(nested1.getDocCount(), equalTo(5L));
                    SingleBucketAggregation filterByBar = nested1.getAggregations().get("filter_by_sku");
                    assertThat(filterByBar.getDocCount(), equalTo(3L));
                    ValueCount barCount = filterByBar.getAggregations().get("sku_count");
                    assertThat(barCount.getValue(), equalTo(3L));
                }
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("idx3").addAggregation(
                nested("nested_0", "category").subAggregation(
                    terms("group_by_category").field("category.name")
                        .subAggregation(
                            reverseNested("to_root").subAggregation(
                                nested("nested_1", "sku").subAggregation(
                                    filter("filter_by_sku", termQuery("sku.sku_type", "bar1")).subAggregation(
                                        nested("nested_2", "sku.colors").subAggregation(
                                            filter("filter_sku_color", termQuery("sku.colors.name", "red")).subAggregation(
                                                reverseNested("reverse_to_sku").path("sku")
                                                    .subAggregation(count("sku_count").field("sku.sku_type"))
                                            )
                                        )
                                    )
                                )
                            )
                        )
                )
            ),
            response -> {
                assertHitCount(response, 1);

                SingleBucketAggregation nested0 = response.getAggregations().get("nested_0");
                assertThat(nested0.getDocCount(), equalTo(3L));
                Terms terms = nested0.getAggregations().get("group_by_category");
                assertThat(terms.getBuckets().size(), equalTo(3));
                for (String bucketName : new String[] { "abc", "klm", "xyz" }) {
                    logger.info("Checking results for bucket {}", bucketName);
                    Terms.Bucket bucket = terms.getBucketByKey(bucketName);
                    assertThat(bucket.getDocCount(), equalTo(1L));
                    SingleBucketAggregation toRoot = bucket.getAggregations().get("to_root");
                    assertThat(toRoot.getDocCount(), equalTo(1L));
                    SingleBucketAggregation nested1 = toRoot.getAggregations().get("nested_1");
                    assertThat(nested1.getDocCount(), equalTo(5L));
                    SingleBucketAggregation filterByBar = nested1.getAggregations().get("filter_by_sku");
                    assertThat(filterByBar.getDocCount(), equalTo(3L));
                    SingleBucketAggregation nested2 = filterByBar.getAggregations().get("nested_2");
                    assertThat(nested2.getDocCount(), equalTo(8L));
                    SingleBucketAggregation filterBarColor = nested2.getAggregations().get("filter_sku_color");
                    assertThat(filterBarColor.getDocCount(), equalTo(2L));
                    SingleBucketAggregation reverseToBar = filterBarColor.getAggregations().get("reverse_to_sku");
                    assertThat(reverseToBar.getDocCount(), equalTo(2L));
                    ValueCount barCount = reverseToBar.getAggregations().get("sku_count");
                    assertThat(barCount.getValue(), equalTo(2L));
                }
            }
        );
    }

    public void testFieldAlias() {
        assertNoFailuresAndResponse(
            prepareSearch("idx1").addAggregation(
                nested("nested1", "nested1").subAggregation(
                    terms("field2").field("nested1.field2")
                        .subAggregation(
                            reverseNested("nested1_to_field1").subAggregation(
                                terms("field1").field("alias").collectMode(randomFrom(SubAggCollectionMode.values()))
                            )
                        )
                )
            ),
            response -> {
                SingleBucketAggregation nested = response.getAggregations().get("nested1");
                Terms nestedTerms = nested.getAggregations().get("field2");
                Terms.Bucket bucket = nestedTerms.getBuckets().iterator().next();

                SingleBucketAggregation reverseNested = bucket.getAggregations().get("nested1_to_field1");
                Terms reverseNestedTerms = reverseNested.getAggregations().get("field1");

                assertThat(((InternalAggregation) reverseNested).getProperty("field1"), sameInstance(reverseNestedTerms));
                assertThat(reverseNestedTerms.getBuckets().size(), equalTo(6));
            }
        );
    }
}
