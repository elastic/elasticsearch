/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.bucketScript;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketScriptIT extends ESIntegTestCase {

    private static final String FIELD_1_NAME = "field1";
    private static final String FIELD_2_NAME = "field2";
    private static final String FIELD_3_NAME = "field3";
    private static final String FIELD_4_NAME = "field4";
    private static final String FIELD_5_NAME = "field5";

    private static int interval;
    private static int numDocs;
    private static int minNumber;
    private static int maxNumber;
    private static long date;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value0 + _value1 + _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            scripts.put("_value0 + _value1 / _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 / value2;
            });

            scripts.put("_value0", vars -> vars.get("_value0"));

            scripts.put("foo + bar + baz", vars -> {
                double foo = (double) vars.get("foo");
                double bar = (double) vars.get("bar");
                double baz = (double) vars.get("baz");
                return foo + bar + baz;
            });

            scripts.put("(_value0 + _value1 + _value2) * factor", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return (value0 + value1 + value2) * (int) vars.get("factor");
            });

            scripts.put("my_script", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            scripts.put("single_input", vars -> {
                double value = (double) vars.get("_value");
                return value;
            });

            scripts.put("return null", vars -> null);

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = randomIntBetween(1, 50);
        numDocs = randomIntBetween(10, 500);
        minNumber = -200;
        maxNumber = 200;
        date = randomLong();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < numDocs; docs++) {
            builders.add(prepareIndex("idx").setSource(newDocBuilder()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder() throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(FIELD_1_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_2_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_3_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_4_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_5_NAME, date);
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScript() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScript2() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 / _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue / field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptWithDateRange() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                dateRange("range").field(FIELD_5_NAME)
                    .addUnboundedFrom(date)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Range range = response.getAggregations().get("range");
                assertThat(range, notNullValue());
                assertThat(range.getName(), equalTo("range"));
                List<? extends Range.Bucket> buckets = range.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Range.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptSingleVariable() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0", Collections.emptyMap()),
                            "field2Sum"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptNamedVars() {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        bucketsPathsMap.put("foo", "field2Sum");
        bucketsPathsMap.put("bar", "field3Sum");
        bucketsPathsMap.put("baz", "field4Sum");
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            bucketsPathsMap,
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "foo + bar + baz", Collections.emptyMap())
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptWithParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("factor", 3);

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "(_value0 + _value1 + _value2) * factor", params);

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScript("seriesArithmetic", script, "field2Sum", "field3Sum", "field4Sum"))
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo((field2SumValue + field3SumValue + field4SumValue) * 3));
                    }
                }
            }
        );
    }

    public void testInlineScriptInsertZeros() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        ).gapPolicy(GapPolicy.INSERT_ZEROS)
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(0.0));
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptReturnNull() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(
                        bucketScript(
                            "nullField",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return null", Collections.emptyMap())
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    assertNull(bucket.getAggregations().get("nullField"));
                }
            }
        );
    }

    public void testStoredScript() {

        putJsonStoredScript(
            "my_script",
            // Script source is not interpreted but it references a pre-defined script from CustomScriptPlugin
            "{ \"script\": {\"lang\": \"" + CustomScriptPlugin.NAME + "\", \"source\": \"my_script\" } }"
        );

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.STORED, null, "my_script", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testUnmapped() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx_unmapped").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Histogram deriv = response.getAggregations().get("histo");
                assertThat(deriv, notNullValue());
                assertThat(deriv.getName(), equalTo("histo"));
                assertThat(deriv.getBuckets().size(), equalTo(0));
            }
        );
    }

    public void testPartiallyUnmapped() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx", "idx_unmapped").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testSingleBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .field("buckets_path", "field2Sum")
            .startObject("script")
            .field("source", "single_input")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg;
        try (var parser = createParser(content)) {
            bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(parser, "seriesArithmetic");
        }

        assertNoFailuresAndResponse(
            prepareSearch("idx", "idx_unmapped").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(bucketScriptAgg)
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue));
                    }
                }
            }
        );
    }

    public void testArrayBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .array("buckets_path", "field2Sum", "field3Sum", "field4Sum")
            .startObject("script")
            .field("source", "_value0 + _value1 + _value2")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg;
        try (var parser = createParser(content)) {
            bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(parser, "seriesArithmetic");
        }

        assertNoFailuresAndResponse(
            prepareSearch("idx", "idx_unmapped").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScriptAgg)
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testObjectBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("buckets_path")
            .field("_value0", "field2Sum")
            .field("_value1", "field3Sum")
            .field("_value2", "field4Sum")
            .endObject()
            .startObject("script")
            .field("source", "_value0 + _value1 + _value2")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg;
        try (var parser = createParser(content)) {
            bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(parser, "seriesArithmetic");
        }

        assertNoFailuresAndResponse(
            prepareSearch("idx", "idx_unmapped").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScriptAgg)
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Sum field2Sum = bucket.getAggregations().get("field2Sum");
                        assertThat(field2Sum, notNullValue());
                        double field2SumValue = field2Sum.value();
                        Sum field3Sum = bucket.getAggregations().get("field3Sum");
                        assertThat(field3Sum, notNullValue());
                        double field3SumValue = field3Sum.value();
                        Sum field4Sum = bucket.getAggregations().get("field4Sum");
                        assertThat(field4Sum, notNullValue());
                        double field4SumValue = field4Sum.value();
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptWithMultiValueAggregationIllegalBucketsPaths() {
        try {
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(percentiles("field2Percentile").field(FIELD_2_NAME).percentiles(10, 50, 90))
                    .subAggregation(percentiles("field3Percentile").field(FIELD_3_NAME).percentiles(10, 50, 90))
                    .subAggregation(percentiles("field4Percentile").field(FIELD_4_NAME).percentiles(10, 50, 90))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Percentile",
                            "field3Percentile",
                            "field4Percentile"
                        )
                    )
            ).get();

            fail("Illegal bucketsPaths was provided but no exception was thrown.");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if ((rootCause instanceof IllegalArgumentException) == false) {
                    throw e;
                }
            } else if ((cause instanceof IllegalArgumentException) == false) {
                throw e;
            }
        }
    }

    public void testInlineScriptWithMultiValueAggregation() {
        int percentile = 90;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(percentiles("field2Percentile").field(FIELD_2_NAME).percentiles(percentile))
                    .subAggregation(percentiles("field3Percentile").field(FIELD_3_NAME).percentiles(percentile))
                    .subAggregation(percentiles("field4Percentile").field(FIELD_4_NAME).percentiles(percentile))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Percentile",
                            "field3Percentile",
                            "field4Percentile"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Percentiles field2Percentile = bucket.getAggregations().get("field2Percentile");
                        assertThat(field2Percentile, notNullValue());
                        double field2PercentileValue = field2Percentile.value(String.valueOf(percentile));
                        Percentiles field3Percentile = bucket.getAggregations().get("field3Percentile");
                        assertThat(field3Percentile, notNullValue());
                        double field3PercentileValue = field3Percentile.value(String.valueOf(percentile));
                        Percentiles field4Percentile = bucket.getAggregations().get("field4Percentile");
                        assertThat(field4Percentile, notNullValue());
                        double field4PercentileValue = field4Percentile.value(String.valueOf(percentile));
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2PercentileValue + field3PercentileValue + field4PercentileValue));
                    }
                }
            }
        );
    }

    public void testInlineScriptWithMultiValueAggregationDifferentBucketsPaths() {
        int percentile10 = 10;
        int percentile50 = 50;
        int percentile90 = 90;
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(percentiles("field2Percentile").field(FIELD_2_NAME))
                    .subAggregation(
                        percentiles("field3Percentile").field(FIELD_3_NAME).percentiles(percentile10, percentile50, percentile90)
                    )
                    .subAggregation(percentiles("field4Percentile").field(FIELD_4_NAME).percentiles(percentile90))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Percentile.10",
                            "field3Percentile.50",
                            "field4Percentile"
                        )
                    )
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();

                for (int i = 0; i < buckets.size(); ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    if (bucket.getDocCount() == 0) {
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, nullValue());
                    } else {
                        Percentiles field2Percentile = bucket.getAggregations().get("field2Percentile");
                        assertThat(field2Percentile, notNullValue());
                        double field2PercentileValue = field2Percentile.value(String.valueOf(percentile10));
                        Percentiles field3Percentile = bucket.getAggregations().get("field3Percentile");
                        assertThat(field3Percentile, notNullValue());
                        double field3PercentileValue = field3Percentile.value(String.valueOf(percentile50));
                        Percentiles field4Percentile = bucket.getAggregations().get("field4Percentile");
                        assertThat(field4Percentile, notNullValue());
                        double field4PercentileValue = field4Percentile.value(String.valueOf(percentile90));
                        SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                        assertThat(seriesArithmetic, notNullValue());
                        double seriesArithmeticValue = seriesArithmetic.value();
                        assertThat(seriesArithmeticValue, equalTo(field2PercentileValue + field3PercentileValue + field4PercentileValue));
                    }
                }
            }
        );
    }
}
