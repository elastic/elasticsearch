/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.AbstractTermsTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

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
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class StringTermsIT extends AbstractTermsTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "s_value";
    private static final String MULTI_VALUED_FIELD_NAME = "s_values";
    private static Map<String, Map<String, Object>> expectedMultiSortBuckets;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    @Before
    public void randomizeOptimizations() {
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = randomBoolean();
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = randomBoolean();
    }

    @After
    public void resetOptimizations() {
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = null;
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = null;
    }

    public static class CustomScriptPlugin extends AggregationTestScriptsPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = super.pluginScripts();

            scripts.put("'foo_' + _value", vars -> "foo_" + (String) vars.get("_value"));
            scripts.put("_value.substring(0,3)", vars -> ((String) vars.get("_value")).substring(0, 3));

            scripts.put("doc['" + MULTI_VALUED_FIELD_NAME + "']", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return doc.get(MULTI_VALUED_FIELD_NAME);
            });

            scripts.put("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value", vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                ScriptDocValues.Strings value = (ScriptDocValues.Strings) doc.get(SINGLE_VALUED_FIELD_NAME);
                return value.getValue();
            });

            scripts.put("42", vars -> 42);

            return scripts;
        }

    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
        );
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(
                prepareIndex("idx").setSource(
                    jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, "val" + i)
                        .field("i", i)
                        .field("constant", 1)
                        .field("tag", i < 5 / 2 + 1 ? "more" : "less")
                        .startArray(MULTI_VALUED_FIELD_NAME)
                        .value("val" + i)
                        .value("val" + (i + 1))
                        .endArray()
                        .endObject()
                )
            );
        }

        getMultiSortDocs(builders);

        assertAcked(
            indicesAdmin().prepareCreate("high_card_idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
        );
        for (int i = 0; i < 100; i++) {
            builders.add(
                prepareIndex("high_card_idx").setSource(
                    jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, "val" + Strings.padStart(i + "", 3, '0'))
                        .startArray(MULTI_VALUED_FIELD_NAME)
                        .value("val" + Strings.padStart(i + "", 3, '0'))
                        .value("val" + Strings.padStart((i + 1) + "", 3, '0'))
                        .endArray()
                        .endObject()
                )
            );
        }
        prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer").get();

        for (int i = 0; i < 2; i++) {
            builders.add(
                prepareIndex("empty_bucket_idx").setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        expectedMultiSortBuckets = new HashMap<>();
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("_term", "val1");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 1d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val2");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 2d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val3");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val4");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 4d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val5");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val6");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val7");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);

        assertAcked(
            indicesAdmin().prepareCreate("sort_idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
        );
        for (int i = 1; i <= 3; i++) {
            builders.add(
                prepareIndex("sort_idx").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val1").field("l", 1).field("d", i).endObject()
                )
            );
            builders.add(
                prepareIndex("sort_idx").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val2").field("l", 2).field("d", i).endObject()
                )
            );
        }
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 2).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 3).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 2).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val6").field("l", 5).field("d", 1).endObject()
            )
        );
        builders.add(
            prepareIndex("sort_idx").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val7").field("l", 5).field("d", 1).endObject()
            )
        );
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        final int minDocCount = randomInt(1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> prepareSearch("high_card_idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .minDocCount(minDocCount)
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
                new TermsAggregationBuilder("terms").field(field).size(10000).collectMode(randomFrom(SubAggCollectionMode.values()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                expectedCardinality[0] = terms.getBuckets().size();
            }
        );

        // Gather terms using partitioned aggregations
        final int numPartitions = randomIntBetween(2, 4);
        Set<String> foundTerms = new HashSet<>();
        for (int partition = 0; partition < numPartitions; partition++) {
            assertNoFailuresAndResponse(
                prepareSearch("idx").addAggregation(
                    new TermsAggregationBuilder("terms").field(field)
                        .includeExclude(new IncludeExclude(partition, numPartitions))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                ),
                response -> {
                    StringTerms terms = response.getAggregations().get("terms");
                    assertThat(terms, notNullValue());
                    assertThat(terms.getName(), equalTo("terms"));
                    for (StringTerms.Bucket bucket : terms.getBuckets()) {
                        assertTrue(foundTerms.add(bucket.getKeyAsString()));
                    }
                }
            );
        }
        assertEquals(expectedCardinality[0], foundTerms.size());
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("foo_val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testMultiValuedFieldWithValueScriptNotUnique() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value.substring(0,3)", Collections.emptyMap()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(1));

                StringTerms.Bucket bucket = terms.getBucketByKey("val");
                assertThat(bucket, notNullValue());
                assertThat(bucket.getKeyAsString(), equalTo("val"));
                assertThat(bucket.getDocCount(), equalTo(5L));
            }
        );
    }

    public void testMultiValuedScript() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .script(
                        new Script(
                            ScriptType.INLINE,
                            CustomScriptPlugin.NAME,
                            "doc['" + MULTI_VALUED_FIELD_NAME + "']",
                            Collections.emptyMap()
                        )
                    )
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(6));

                for (int i = 0; i < 6; i++) {
                    Terms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    if (i == 0 || i == 5) {
                        assertThat(bucket.getDocCount(), equalTo(1L));
                    } else {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                    }
                }
            }
        );
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(6));

                for (int i = 0; i < 6; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("foo_val" + i));
                    if (i == 0 || i == 5) {
                        assertThat(bucket.getDocCount(), equalTo(1L));
                    } else {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                    }
                }
            }
        );
    }

    /*
     *
     * [foo_val0, foo_val1] [foo_val1, foo_val2] [foo_val2, foo_val3] [foo_val3,
     * foo_val4] [foo_val4, foo_val5]
     *
     *
     * foo_val0 - doc_count: 1 - val_count: 2 foo_val1 - doc_count: 2 -
     * val_count: 4 foo_val2 - doc_count: 2 - val_count: 4 foo_val3 - doc_count:
     * 2 - val_count: 4 foo_val4 - doc_count: 2 - val_count: 4 foo_val5 -
     * doc_count: 1 - val_count: 2
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
                    .executionHint(randomExecutionHint())
                    .script(script)
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testScriptSingleValueExplicitSingleValue() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
            Collections.emptyMap()
        );

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(randomExecutionHint())
                    .script(script)
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testScriptMultiValued() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx")

                .addAggregation(
                    new TermsAggregationBuilder("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                        .executionHint(randomExecutionHint())
                        .script(
                            new Script(
                                ScriptType.INLINE,
                                CustomScriptPlugin.NAME,
                                "doc['" + MULTI_VALUED_FIELD_NAME + "']",
                                Collections.emptyMap()
                            )
                        )
                ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(6));

                for (int i = 0; i < 6; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
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
            prepareSearch("idx", "idx_unmapped").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                for (int i = 0; i < 5; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        );
    }

    public void testStringTermsNestedIntoPerBucketAggregator() throws Exception {
        // no execution hint so that the logic that decides whether or not to use ordinals is executed
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                filter("filter", termQuery(MULTI_VALUED_FIELD_NAME, "val3")).subAggregation(
                    new TermsAggregationBuilder("terms").field(MULTI_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                )
            ),
            response -> {
                assertThat(response.getFailedShards(), equalTo(0));

                Filter filter = response.getAggregations().get("filter");

                StringTerms terms = filter.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(3));

                for (int i = 2; i <= 4; i++) {
                    StringTerms.Bucket bucket = terms.getBucketByKey("val" + i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(i == 3 ? 2L : 1L));
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByIllegalAgg() throws Exception {
        boolean asc = true;
        try {
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("inner_terms>avg", asc))
                    .subAggregation(
                        new TermsAggregationBuilder("inner_terms").field(MULTI_VALUED_FIELD_NAME).subAggregation(avg("avg").field("i"))
                    )
            ).get();
            fail("Expected an exception");
        } catch (SearchPhaseExecutionException e) {
            ElasticsearchException[] rootCauses = e.guessRootCauses();
            if (rootCauses.length == 1) {
                ElasticsearchException rootCause = rootCauses[0];
                if (rootCause instanceof AggregationExecutionException aggException) {
                    assertThat(aggException.getMessage(), startsWith("Invalid aggregation order path"));
                } else {
                    throw e;
                }
            } else if (e.getCause() instanceof IllegalArgumentException) {
                // Thrown when the terms agg runs as a filters agg
                assertThat(
                    e.getCause().getMessage(),
                    equalTo("Invalid aggregation order path [inner_terms>avg]. Can't sort by a descendant of a [sterms] aggregation [avg]")
                );
            } else {
                throw e;
            }
        }
    }

    public void testSingleValuedFieldOrderedBySingleBucketSubAggregationAsc() throws Exception {
        boolean asc = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter", asc))
                    .subAggregation(filter("filter", QueryBuilders.matchAllQuery()))
            ),
            response -> {
                StringTerms tags = response.getAggregations().get("tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<StringTerms.Bucket> iters = tags.getBuckets().iterator();

                Terms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "less" : "more"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                Filter filter = tag.getAggregations().get("filter");
                assertThat(filter, notNullValue());
                assertThat(filter.getDocCount(), equalTo(asc ? 2L : 3L));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "more" : "less"));
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
                new TermsAggregationBuilder("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>filter2>stats.max", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter("filter2", QueryBuilders.matchAllQuery()).subAggregation(stats("stats").field("i"))
                        )
                    )
            ),
            response -> {
                StringTerms tags = response.getAggregations().get("tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<StringTerms.Bucket> iters = tags.getBuckets().iterator();

                // the max for "more" is 2
                // the max for "less" is 4

                StringTerms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "more" : "less"));
                assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter2 = filter1.getAggregations().get("filter2");
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
                Stats stats = filter2.getAggregations().get("stats");
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "less" : "more"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
                filter2 = filter1.getAggregations().get("filter2");
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
                stats = filter2.getAggregations().get("stats");
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
            }
        );
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialChars() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>" + filter2Name + ">" + statsName + ".max", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(stats(statsName).field("i"))
                        )
                    )
            ),
            response -> {
                StringTerms tags = response.getAggregations().get("tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<StringTerms.Bucket> iters = tags.getBuckets().iterator();

                // the max for "more" is 2
                // the max for "less" is 4

                StringTerms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "more" : "less"));
                assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter2 = filter1.getAggregations().get(filter2Name);
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
                Stats stats = filter2.getAggregations().get(statsName);
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "less" : "more"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
                filter2 = filter1.getAggregations().get(filter2Name);
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
                stats = filter2.getAggregations().get(statsName);
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
            }
        );
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialCharsNoDotNotation() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>" + filter2Name + ">" + statsName + "[max]", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(stats(statsName).field("i"))
                        )
                    )
            ),
            response -> {
                StringTerms tags = response.getAggregations().get("tags");
                assertThat(tags, notNullValue());
                assertThat(tags.getName(), equalTo("tags"));
                assertThat(tags.getBuckets().size(), equalTo(2));

                Iterator<StringTerms.Bucket> iters = tags.getBuckets().iterator();

                // the max for "more" is 2
                // the max for "less" is 4

                StringTerms.Bucket tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "more" : "less"));
                assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
                Filter filter2 = filter1.getAggregations().get(filter2Name);
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
                Stats stats = filter2.getAggregations().get(statsName);
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

                tag = iters.next();
                assertThat(tag, notNullValue());
                assertThat(tag.getKeyAsString(), equalTo(asc ? "less" : "more"));
                assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
                filter1 = tag.getAggregations().get("filter1");
                assertThat(filter1, notNullValue());
                assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
                filter2 = filter1.getAggregations().get(filter2Name);
                assertThat(filter2, notNullValue());
                assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
                stats = filter2.getAggregations().get(statsName);
                assertThat(stats, notNullValue());
                assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
            }
        );
    }

    public void testSingleValuedFieldOrderedByMissingSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
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
                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("values", true))
                        .subAggregation(
                            new TermsAggregationBuilder("values").field("i").collectMode(randomFrom(SubAggCollectionMode.values()))
                        )
                ).get();

                fail(
                    "Expected search to fail when trying to sort terms aggregation by sug-aggregation "
                        + "which is not of a metrics or single-bucket type"
                );

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithUnknownMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                prepareSearch(index).addAggregation(
                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("stats.foo", true))
                        .subAggregation(stats("stats").field("i"))
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
                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("stats", true))
                        .subAggregation(stats("stats").field("i"))
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
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field("i"))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                int i = 0;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    Stats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                    i++;
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field("i"))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                int i = 4;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    Stats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                    i--;
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.sum_of_squares", asc))
                    .subAggregation(extendedStats("stats").field("i"))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                int i = 0;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    ExtendedStats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));
                    i++;
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedByStatsAggAscWithTermsSubAgg() throws Exception {
        boolean asc = true;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.sum_of_squares", asc))
                    .subAggregation(extendedStats("stats").field("i"))
                    .subAggregation(
                        new TermsAggregationBuilder("subTerms").field("s_values").collectMode(randomFrom(SubAggCollectionMode.values()))
                    )
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(5));

                int i = 0;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo("val" + i));
                    assertThat(bucket.getDocCount(), equalTo(1L));

                    ExtendedStats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    assertThat(stats.getMax(), equalTo((double) i));

                    StringTerms subTermsAgg = bucket.getAggregations().get("subTerms");
                    assertThat(subTermsAgg, notNullValue());
                    assertThat(subTermsAgg.getBuckets().size(), equalTo(2));
                    int j = i;
                    for (StringTerms.Bucket subBucket : subTermsAgg.getBuckets()) {
                        assertThat(subBucket, notNullValue());
                        assertThat(subBucket.getKeyAsString(), equalTo("val" + j));
                        assertThat(subBucket.getDocCount(), equalTo(1L));
                        j++;
                    }
                    i++;
                }
            }
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsDesc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val4", "val3", "val7", "val6", "val5" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDescAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val5", "val6", "val7", "val3", "val4", "val2", "val1" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", false), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val4", "val5", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, BucketOrder.count(true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val5", "val4", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("sum_d", true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedByThreeCriteria() throws Exception {
        String[] expectedKeys = new String[] { "val2", "val1", "val4", "val5", "val3", "val6", "val7" };
        assertMultiSortResponse(
            expectedKeys,
            BucketOrder.count(false),
            BucketOrder.aggregation("sum_d", false),
            BucketOrder.aggregation("avg_l", false)
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true));
    }

    private void assertMultiSortResponse(String[] expectedKeys, BucketOrder... order) {
        assertNoFailuresAndResponse(
            prepareSearch("sort_idx").addAggregation(
                new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.compound(order))
                    .subAggregation(avg("avg_l").field("l"))
                    .subAggregation(sum("sum_d").field("d"))
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(expectedKeys.length));

                int i = 0;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo(expectedKeys[i]));
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

    public void testIndexMetaField() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx", "empty_bucket_idx").addAggregation(
                new TermsAggregationBuilder("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(randomExecutionHint())
                    .field(IndexFieldMapper.NAME)
            ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(2));

                int i = 0;
                for (StringTerms.Bucket bucket : terms.getBuckets()) {
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo(i == 0 ? "idx" : "empty_bucket_idx"));
                    assertThat(bucket.getDocCount(), equalTo(i == 0 ? 5L : 2L));
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
            prepareCreate("cache_test_idx").setMapping("d", "type=keyword")
                .setSettings(indexSettings(1, 1).put("requests.cache.enable", true))
        );
        indexRandom(
            true,
            prepareIndex("cache_test_idx").setId("1").setSource("s", "foo"),
            prepareIndex("cache_test_idx").setId("2").setSource("s", "bar")
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
                        .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
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

    public void testScriptWithValueType() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("terms").script(
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "42", Collections.emptyMap())
                ).userValueTypeHint(randomFrom(ValueType.NUMERIC, ValueType.NUMBER))
            );
        String source = builder.toString();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            assertNoFailuresAndResponse(
                prepareSearch("idx").setSource(new SearchSourceBuilder().parseXContent(parser, true, nf -> false)),
                response -> {
                    LongTerms terms = response.getAggregations().get("terms");
                    assertThat(terms, notNullValue());
                    assertThat(terms.getName(), equalTo("terms"));
                    assertThat(terms.getBuckets().size(), equalTo(1));
                }
            );
        }

        String invalidValueType = source.replaceAll("\"value_type\":\"n.*\"", "\"value_type\":\"foobar\"");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidValueType)) {
            XContentParseException ex = expectThrows(
                XContentParseException.class,
                () -> prepareSearch("idx").setSource(new SearchSourceBuilder().parseXContent(parser, true, nf -> false)).get()
            );
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(ex.getCause().getMessage(), containsString("Unknown value type [foobar]"));
        }
    }

    public void testOrderByKey() throws Exception {
        Map<String, long[]> data = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            assertAcked(
                indicesAdmin().prepareCreate("idx" + i).setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", "filter", "type=boolean")
            );
            List<IndexRequestBuilder> builders = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                String val = "val" + random().nextInt(1000);
                boolean filter = randomBoolean();
                long[] counter = data.computeIfAbsent(val, s -> new long[] { 0 });
                if (filter == false) {
                    counter[0]++;
                }
                builders.add(
                    prepareIndex("idx" + i).setSource(
                        jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, val).field("filter", filter).endObject()
                    )
                );
            }
            indexRandom(true, builders);
        }
        List<String> allKeys = new ArrayList<>(data.keySet());
        List<String> keysMinDocCount1 = allKeys.stream().filter(key -> data.get(key)[0] > 0).collect(Collectors.toList());
        List<String> keysMinDocCount2 = allKeys.stream().filter(key -> data.get(key)[0] > 1).collect(Collectors.toList());
        // test for different batch sizes to exercise partial reduces
        for (int batchReduceSize = 2; batchReduceSize < 6; batchReduceSize++) {
            // with min_doc_count = 0
            allKeys.sort(String::compareTo);
            assertOrderByKeyResponse(allKeys, data, true, 0, batchReduceSize);
            Collections.reverse(allKeys);
            assertOrderByKeyResponse(allKeys, data, false, 0, batchReduceSize);
            // with min_doc_count = 1
            keysMinDocCount1.sort(String::compareTo);
            assertOrderByKeyResponse(keysMinDocCount1, data, true, 1, batchReduceSize);
            Collections.reverse(keysMinDocCount1);
            assertOrderByKeyResponse(keysMinDocCount1, data, false, 1, batchReduceSize);
            // with min_doc_count = 2
            keysMinDocCount2.sort(String::compareTo);
            assertOrderByKeyResponse(keysMinDocCount2, data, true, 2, batchReduceSize);
            Collections.reverse(keysMinDocCount2);
            assertOrderByKeyResponse(keysMinDocCount2, data, false, 2, batchReduceSize);
        }
        for (int i = 0; i < 5; i++) {
            assertAcked(indicesAdmin().prepareDelete("idx" + i));
        }
    }

    private void assertOrderByKeyResponse(
        List<String> keys,
        Map<String, long[]> counts,
        boolean asc,
        int minDocCount,
        int batchReduceSize
    ) {
        int size = randomIntBetween(1, keys.size());
        long sumOtherCount = 0;
        for (int i = size; i < keys.size(); i++) {
            sumOtherCount += counts.get(keys.get(i))[0];
        }
        final long finalSumOtherCount = sumOtherCount;
        assertNoFailuresAndResponse(
            prepareSearch("idx0", "idx1", "idx2", "idx3", "idx4").setBatchedReduceSize(batchReduceSize)
                .setQuery(QueryBuilders.termQuery("filter", false))
                .addAggregation(
                    new TermsAggregationBuilder("terms").field(SINGLE_VALUED_FIELD_NAME)
                        .size(size)
                        .shardSize(500)
                        .minDocCount(minDocCount)
                        .order(BucketOrder.key(asc))
                ),
            response -> {
                StringTerms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                assertThat(terms.getName(), equalTo("terms"));
                assertThat(terms.getBuckets().size(), equalTo(size));
                assertThat(terms.getSumOfOtherDocCounts(), equalTo(finalSumOtherCount));

                for (int i = 0; i < size; i++) {
                    StringTerms.Bucket bucket = terms.getBuckets().get(i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsString(), equalTo(keys.get(i)));
                    assertThat(bucket.getDocCount(), equalTo(counts.get(keys.get(i))[0]));
                }
            }
        );
    }

    public void testGlobalAggregationWithScore() throws Exception {
        assertAcked(prepareCreate("global").setMapping("keyword", "type=keyword"));
        indexRandom(
            true,
            prepareIndex("global").setSource("keyword", "a"),
            prepareIndex("global").setSource("keyword", "c"),
            prepareIndex("global").setSource("keyword", "e")
        );
        String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
        Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING)
                .executionHint(executionHint)
                .collectMode(collectionMode)
                .field("keyword")
                .order(BucketOrder.key(true))
                .subAggregation(
                    new TermsAggregationBuilder("sub_terms").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .collectMode(collectionMode)
                        .field("keyword")
                        .order(BucketOrder.key(true))
                        .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"))
                )
        );
        assertNoFailuresAndResponse(prepareSearch("global").addAggregation(globalBuilder), response -> {
            InternalGlobal result = response.getAggregations().get("global");
            InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
            assertThat(terms.getBuckets().size(), equalTo(3));
            for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                InternalMultiBucketAggregation<?, ?> subTerms = bucket.getAggregations().get("sub_terms");
                assertThat(subTerms.getBuckets().size(), equalTo(1));
                MultiBucketsAggregation.Bucket subBucket = subTerms.getBuckets().get(0);
                InternalTopHits topHits = subBucket.getAggregations().get("top_hits");
                assertThat(topHits.getHits().getHits().length, equalTo(1));
                for (SearchHit hit : topHits.getHits()) {
                    assertThat(hit.getScore(), greaterThan(0f));
                }
            }
        });
    }
}
