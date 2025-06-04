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
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class LongTermsIT extends AbstractTermsTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends AggregationTestScriptsPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = super.pluginScripts();

            scripts.put("floor(_value / 1000 + 1)", vars -> Math.floor((double) vars.get("_value") / 1000 + 1));

            scripts.put("doc['" + MULTI_VALUED_FIELD_NAME + "']", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return doc.get(MULTI_VALUED_FIELD_NAME);
            });

            scripts.put("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Longs value = (ScriptDocValues.Longs) doc.get(SINGLE_VALUED_FIELD_NAME);
                return value.getValue();
            });

            return scripts;
        }

    }

    private static final int NUM_DOCS = 5; // TODO randomize the size?
    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";
    private static HashMap<Long, Map<String, Object>> expectedMultiSortBuckets;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx", "high_card_idx");
        IndexRequestBuilder[] lowCardBuilders = new IndexRequestBuilder[NUM_DOCS];
        for (int i = 0; i < lowCardBuilders.length; i++) {
            lowCardBuilders[i] = prepareIndex("idx").setSource(
                jsonBuilder().startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, i)
                    .startArray(MULTI_VALUED_FIELD_NAME)
                    .value(i)
                    .value(i + 1)
                    .endArray()
                    .field("num_tag", i < lowCardBuilders.length / 2 + 1 ? 1 : 0) // used to test order by single-bucket sub agg
                    .field("constant", 1)
                    .endObject()
            );
        }
        indexRandom(true, lowCardBuilders);
        IndexRequestBuilder[] highCardBuilders = new IndexRequestBuilder[100]; // TODO randomize the size?
        for (int i = 0; i < highCardBuilders.length; i++) {
            highCardBuilders[i] = prepareIndex("high_card_idx").setSource(
                jsonBuilder().startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, i)
                    .startArray(MULTI_VALUED_FIELD_NAME)
                    .value(i)
                    .value(i + 1)
                    .endArray()
                    .endObject()
            );

        }
        indexRandom(true, highCardBuilders);
        createIndex("idx_unmapped");

        assertAcked(prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer"));
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(
                prepareIndex("empty_bucket_idx").setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }

        getMultiSortDocs(builders);

        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));
        ensureSearchable();
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        expectedMultiSortBuckets = new HashMap<>();
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("_term", 1L);
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 1d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 2L);
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 2d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 3L);
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 4L);
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 4d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 5L);
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 6L);
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 7L);
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((Long) bucketProps.get("_term"), bucketProps);

        createIndex("sort_idx");
        for (int i = 1; i <= 3; i++) {
            builders.add(
                prepareIndex("sort_idx").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 1).field("l", 1).field("d", i).endObject()
                )
            );
            builders.add(
                prepareIndex("sort_idx").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 2).field("l", 2).field("d", i).endObject()
                )
            );
        }
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 3).field("l", 3).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 3).field("l", 3).field("d", 2).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4).field("l", 3).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4).field("l", 3).field("d", 3).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5).field("l", 5).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5).field("l", 5).field("d", 2).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 6).field("l", 5).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 7).field("l", 5).field("d", 1).endObject()
            )
        );
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> prepareSearch("high_card_idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .minDocCount(randomInt(1))
                    .size(0)
            ).get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [terms]"));
    }

    public void testSingleValueFieldWithPartitionedFiltering() throws Exception {
        runTestFieldWithPartitionedFiltering(SINGLE_VALUED_FIELD_NAME);
    }

    public void testMultiValueFieldWithPartitionedFiltering() throws Exception {
        runTestFieldWithPartitionedFiltering(MULTI_VALUED_FIELD_NAME);
    }

    private void runTestFieldWithPartitionedFiltering(String field) throws Exception {
        // Find total number of unique terms
        int[] expectedCardinality = new int[1];
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(field).collectMode(randomFrom(SubAggCollectionMode.values()))
            ),
            response -> {
                assertNoFailures(response);
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                expectedCardinality[0] = terms.getBuckets().size();
            }
        );

        // Gather terms using partitioned aggregations
        final int numPartitions = randomIntBetween(2, 4);
        Set<Number> foundTerms = new HashSet<>();
        for (int partition = 0; partition < numPartitions; partition++) {
            assertNoFailuresAndResponse(
                prepareSearch("idx").addAggregation(
                    new TermsAggregationBuilder("terms").field(field)
                        .includeExclude(new IncludeExclude(partition, numPartitions))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                ),
                response -> {
                    LongTerms terms = response.getAggregations().get("terms");
                    assertThat(terms, notNullValue());
                    assertThat(terms.getName(), equalTo("terms"));
                    for (LongTerms.Bucket bucket : terms.getBuckets()) {
                        assertFalse(foundTerms.contains(bucket.getKeyAsNumber()));
                        foundTerms.add(bucket.getKeyAsNumber());
                    }
                }
            );
        }
        assertEquals(expectedCardinality[0], foundTerms.size());
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", Collections.emptyMap()))
            ),
            response -> {
                // Scripts force the results to doubles
                DoubleTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    DoubleTerms.Bucket bucket = terms.getBucketByKey("" + (i + 1d));
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + (i + 1d)));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i + 1));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value - 1", Collections.emptyMap()))
            ),
            response -> {
                // Scripts force the results to doubles
                DoubleTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(6));

                for (int i = 0; i < 6; i++) {
                    DoubleTerms.Bucket bucket = terms.getBucketByKey("" + (i - 1d));
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + (i - 1d)));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i - 1));
                    if (i == 0 || i == 5) {
                        assertThat(bucket.getDocCount(), equalTo(1L));
                    } else {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                    }
                }
            }
        );
    }

    public void testMultiValuedFieldWithValueScriptNotUnique() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "floor(_value / 1000 + 1)", Collections.emptyMap()))
            ),
            response -> {
                // The script always converts long to double
                DoubleTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(1));

                DoubleTerms.Bucket bucket = terms.getBucketByKey("1.0");
                assertThat(bucket, notNullValue());
                assertThat(bucket.getKeyAsString(), equalTo("1.0"));
                assertThat(bucket.getKeyAsNumber().intValue(), equalTo(1));
                assertThat(bucket.getDocCount(), equalTo(5L));
            }
        );
    }

    /*

    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]

    1 - count: 1 - sum: 1
    2 - count: 2 - sum: 4
    3 - count: 2 - sum: 6
    4 - count: 2 - sum: 8
    5 - count: 2 - sum: 10
    6 - count: 1 - sum: 6

    */

    public void testScriptSingleValue() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
            Collections.emptyMap()
        );

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .userValueTypeHint(ValueType.LONG)
                    .script(script)
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
                    assertThat(bucket.getKeyAsNumber(), instanceOf(Long.class));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testScriptMultiValued() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + MULTI_VALUED_FIELD_NAME + "']",
            Collections.emptyMap()
        );

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .userValueTypeHint(ValueType.LONG)
                    .script(script)
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(6));

                for (int i = 0; i < 6; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
                    if (i == 0 || i == 5) {
                        assertThat(bucket.getDocCount(), equalTo(1L));
                    } else {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                    }
                }
            }
        );
    }

    public void testPartiallyUnmapped() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx_unmapped", "idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values()))
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testPartiallyUnmappedWithFormat() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx_unmapped", "idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .format("0000")
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    String key = Strings.format("%04d", i);
                    LongTerms.Bucket bucket = terms.getBucketByKey(key);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo(key));
                    assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscWithTermsSubAgg() throws Exception {
        boolean asc = true;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("avg_i", asc))
                    .subAggregation(avg("avg_i").field(SINGLE_VALUED_FIELD_NAME))
                    .subAggregation(
                        new TermsAggregationBuilder("subTerms").field(MULTI_VALUED_FIELD_NAME)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                    )
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    Avg avg = bucket.getAggregations().get("avg_i");
                    assertThat(avg, notNullValue());
                    assertThat(avg.getValue(), equalTo((double) i));

                    LongTerms subTermsAgg = bucket.getAggregations().get("subTerms");
                    assertThat(subTermsAgg, notNullValue());
                    assertThat(subTermsAgg.getBuckets().size(), equalTo(2));
                    int j = i;
                    for (LongTerms.Bucket subBucket : subTermsAgg.getBuckets()) {
                        assertThat(subBucket, notNullValue());
                        assertThat(subBucket.getKeyAsString(), equalTo(String.valueOf(j)));
                        assertThat(subBucket.getDocCount(), equalTo(1L));
                        j++;
                    }
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedBySingleBucketSubAggregationAsc() throws Exception {
        boolean asc = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("num_tags").field("num_tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter", asc))
                    .subAggregation(filter("filter", QueryBuilders.matchAllQuery()))
            ),
            response -> {
                LongTerms tags = response.getAggregations().get("num_tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("num_tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<LongTerms.Bucket> iters = tags.getBuckets().iterator();

                LongTerms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "0" : "1"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                SingleBucketAggregation filter = tag.getAggregations().get("filter");
                assertThat(filter, notNullValue());
                assertThat(filter.getDocCount(), equalTo(asc ? 2L : 3L));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "1" : "0"));
                assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
                filter = tag.getAggregations().get("filter");
                assertThat(filter, notNullValue());
                assertThat(filter.getDocCount(), equalTo(asc ? 3L : 2L));
            }
        );
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevels() throws Exception {
        boolean asc = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("tags").field("num_tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>filter2>max", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter("filter2", QueryBuilders.matchAllQuery()).subAggregation(max("max").field(SINGLE_VALUED_FIELD_NAME))
                        )
                    )
            ),
            response -> {
                LongTerms tags = response.getAggregations().get("tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<LongTerms.Bucket> iters = tags.getBuckets().iterator();

                // the max for "1" is 2
                // the max for "0" is 4

                LongTerms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "1" : "0"));
                assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
                SingleBucketAggregation filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
                SingleBucketAggregation filter2 = filter1.getAggregations().get("filter2");
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
                Max max = filter2.getAggregations().get("max");
                assertThat(max, notNullValue());
                assertThat(max.value(), equalTo(asc ? 2.0 : 4.0));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "0" : "1"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
                filter2 = filter1.getAggregations().get("filter2");
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
                max = filter2.getAggregations().get("max");
                assertThat(max, notNullValue());
                assertThat(max.value(), equalTo(asc ? 4.0 : 2.0));
            }
        );
    }

    public void testSingleValuedFieldOrderedByMissingSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("avg_i", true))
                ).get();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation that doesn't exist");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByNonMetricsOrMultiBucketSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("num_tags", true))
                        .subAggregation(
                            new TermsAggregationBuilder("num_tags").field("num_tags").collectMode(randomFrom(SubAggCollectionMode.values()))
                        )
                ).get();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation which is not of a metrics type");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithUnknownMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("stats.foo", true))
                        .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                ).get();

                fail(
                    "Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "with an unknown specified metric to order by"
                );

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithoutMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("stats", true))
                        .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                ).get();

                fail(
                    "Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "where the metric name is not specified"
                );

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    Stats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 4; i >= 0; i--) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    Stats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.variance", asc))
                    .subAggregation(extendedStats("stats").field(SINGLE_VALUED_FIELD_NAME))
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    LongTerms.Bucket bucket = terms.getBucketByKey("" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    ExtendedStats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsDesc() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 4, 3, 7, 6, 5 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsAsc() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDescAndTermsAsc() throws Exception {
        long[] expectedKeys = new long[] { 5, 6, 7, 3, 4, 2, 1 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", false), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        long[] expectedKeys = new long[] { 6, 7, 3, 4, 5, 1, 2 };
        assertMultiSortResponse(expectedKeys, BucketOrder.count(true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        long[] expectedKeys = new long[] { 6, 7, 3, 5, 4, 1, 2 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("sum_d", true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedByThreeCriteria() throws Exception {
        long[] expectedKeys = new long[] { 2, 1, 4, 5, 3, 6, 7 };
        assertMultiSortResponse(
            expectedKeys,
            BucketOrder.count(false),
            BucketOrder.aggregation("sum_d", false),
            BucketOrder.aggregation("avg_l", false)
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true));
    }

    private void assertMultiSortResponse(long[] expectedKeys, BucketOrder... order) {
        assertNoFailuresAndResponse(
            prepareSearch("sort_idx").addAggregation(
                new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.compound(order))
                    .subAggregation(avg("avg_l").field("l"))
                    .subAggregation(sum("sum_d").field("d"))
            ),
            response -> {
                LongTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(expectedKeys.length));

                int i = 0;
                for (LongTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(expectedKeys[i])));
                    assertThat(bucket.getDocCount(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("_count")));
                    Avg avg = bucket.getAggregations().get("avg_l");
                    assertThat(avg, notNullValue());
                    assertThat(avg.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("avg_l")));
                    Sum sum = bucket.getAggregations().get("sum_d");
                    assertThat(sum, notNullValue());
                    assertThat(sum.value(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("sum_d")));
                    i++;
                }
            }
        );
    }

    public void testOtherDocCount() {
        testOtherDocCount(SINGLE_VALUED_FIELD_NAME, MULTI_VALUED_FIELD_NAME);
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long").setSettings(indexSettings(1, 1).put("requests.cache.enable", true))
        );
        indexRandom(
            true,
            prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    new TermsAggregationBuilder("terms").field("d")
                        .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "Math.random()", Collections.emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    new TermsAggregationBuilder("terms").field("d")
                        .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", Collections.emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        assertNoFailures(prepareSearch("cache_test_idx").setSize(0).addAggregation(new TermsAggregationBuilder("terms").field("d")));

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(2L)
        );
    }
}
