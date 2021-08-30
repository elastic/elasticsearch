/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.bucketSelector;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.derivative;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketSelectorIT extends ESIntegTestCase {

    private static final String FIELD_1_NAME = "field1";
    private static final String FIELD_2_NAME = "field2";
    private static final String FIELD_3_NAME = "field3";
    private static final String FIELD_4_NAME = "field4";

    private static int interval;
    private static int numDocs;
    private static int minNumber;
    private static int maxNumber;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                return Double.isNaN(value0) ? false : (value0 + value1 > 100);
            });

            scripts.put("Double.isNaN(_value0) ? true : (_value0 < 10000)", vars -> {
                double value0 = (double) vars.get("_value0");
                return Double.isNaN(value0) ? true : (value0 < 10000);
            });

            scripts.put("Double.isNaN(_value0) ? false : (_value0 > 10000)", vars -> {
                double value0 = (double) vars.get("_value0");
                return Double.isNaN(value0) ? false : (value0 > 10000);
            });

            scripts.put("Double.isNaN(_value0) ? false : (_value0 < _value1)", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                return Double.isNaN(value0) ? false : (value0 < value1);
            });

            scripts.put("Double.isNaN(_value0) ? false : (_value0 > 100)", vars -> {
                double value0 = (double) vars.get("_value0");
                return Double.isNaN(value0) ? false : (value0 > 10000);
            });

            scripts.put("Double.isNaN(my_value1) ? false : (my_value1 + my_value2 > 100)", vars -> {
                double myValue1 = (double) vars.get("my_value1");
                double myValue2 = (double) vars.get("my_value2");
                return Double.isNaN(myValue1) ? false : (myValue1 + myValue2 > 100);
            });

            scripts.put("Double.isNaN(_value0) ? false : (_value0 + _value1 > threshold)", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                int threshold = (int) vars.get("threshold");
                return Double.isNaN(value0) ? false : (value0 + value1 > threshold);
            });

            scripts.put("_value0 + _value1 > 100", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                return (value0 + value1 > 100);
            });

            scripts.put("my_script", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                return Double.isNaN(value0) ? false : (value0 + value1 > 100);
            });

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        createIndex("idx_with_gaps");

        interval = randomIntBetween(1, 50);
        numDocs = randomIntBetween(10, 500);
        minNumber = -200;
        maxNumber = 200;

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < numDocs; docs++) {
            builders.add(client().prepareIndex("idx", "type").setSource(newDocBuilder()));
        }
        builders.add(client().prepareIndex("idx_with_gaps", "type").setSource(newDocBuilder(1, 1, 0, 0)));
        builders.add(client().prepareIndex("idx_with_gaps", "type").setSource(newDocBuilder(1, 2, 0, 0)));
        builders.add(client().prepareIndex("idx_with_gaps", "type").setSource(newDocBuilder(3, 1, 0, 0)));
        builders.add(client().prepareIndex("idx_with_gaps", "type").setSource(newDocBuilder(3, 3, 0, 0)));

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder() throws IOException {
        return newDocBuilder(
            randomIntBetween(minNumber, maxNumber),
            randomIntBetween(minNumber, maxNumber),
            randomIntBetween(minNumber, maxNumber),
            randomIntBetween(minNumber, maxNumber)
        );
    }

    private XContentBuilder newDocBuilder(int field1Value, int field2Value, int field3Value, int field4Value) throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(FIELD_1_NAME, field1Value);
        jsonBuilder.field(FIELD_2_NAME, field2Value);
        jsonBuilder.field(FIELD_3_NAME, field3Value);
        jsonBuilder.field(FIELD_4_NAME, field4Value);
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScript() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testInlineScriptNoBucketsPruned() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? true : (_value0 < 10000)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, lessThan(10000.0));
        }
    }

    public void testInlineScriptNoBucketsLeft() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 > 10000)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }

    public void testInlineScript2() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 < _value1)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field3SumValue - field2SumValue, greaterThan(0.0));
        }
    }

    public void testInlineScriptSingleVariable() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 > 100)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            assertThat(field2SumValue, greaterThan(100.0));
        }
    }

    public void testInlineScriptNamedVars() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(my_value1) ? false : (my_value1 + my_value2 > 100)",
            Collections.emptyMap()
        );

        Map<String, String> bucketPathsMap = new HashMap<>();
        bucketPathsMap.put("my_value1", "field2Sum");
        bucketPathsMap.put("my_value2", "field3Sum");

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", bucketPathsMap, script))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testInlineScriptWithParams() {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 + _value1 > threshold)",
            Collections.singletonMap("threshold", 100)
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testInlineScriptInsertZeros() {
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 > 100", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum").gapPolicy(GapPolicy.INSERT_ZEROS))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testStoredScript() {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("my_script")
                // Source is not interpreted but my_script is defined in CustomScriptPlugin
                .setContent(
                    new BytesArray(
                        "{ \"script\": { \"lang\": \""
                            + CustomScriptPlugin.NAME
                            + "\", "
                            + "\"source\": \"Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)\" } }"
                    ),
                    XContentType.JSON
                )
        );

        Script script = new Script(ScriptType.STORED, null, "my_script", Collections.emptyMap());

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testUnmapped() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(bucketSelector("bucketSelector", script, "field2Sum", "field3Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            Sum field2Sum = bucket.getAggregations().get("field2Sum");
            assertThat(field2Sum, notNullValue());
            double field2SumValue = field2Sum.getValue();
            Sum field3Sum = bucket.getAggregations().get("field3Sum");
            assertThat(field3Sum, notNullValue());
            double field3SumValue = field3Sum.getValue();
            assertThat(field2SumValue + field3SumValue, greaterThan(100.0));
        }
    }

    public void testEmptyBuckets() {
        SearchResponse response = client().prepareSearch("idx_with_gaps")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(1)
                    .subAggregation(
                        histogram("inner_histo").field(FIELD_1_NAME)
                            .interval(1)
                            .extendedBounds(1L, 4L)
                            .minDocCount(0)
                            .subAggregation(derivative("derivative", "_count").gapPolicy(GapPolicy.INSERT_ZEROS))
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("1.0"));
        Histogram innerHisto = bucket.getAggregations().get("inner_histo");
        assertThat(innerHisto, notNullValue());
        List<? extends Histogram.Bucket> innerBuckets = innerHisto.getBuckets();
        assertThat(innerBuckets, notNullValue());
        assertThat(innerBuckets.size(), equalTo(4));
        for (int i = 0; i < innerBuckets.size(); i++) {
            Histogram.Bucket innerBucket = innerBuckets.get(i);
            if (i == 0) {
                assertThat(innerBucket.getAggregations().get("derivative"), nullValue());
            } else {
                assertThat(innerBucket.getAggregations().get("derivative"), notNullValue());
            }
        }

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("2.0"));
        innerHisto = bucket.getAggregations().get("inner_histo");
        assertThat(innerHisto, notNullValue());
        innerBuckets = innerHisto.getBuckets();
        assertThat(innerBuckets, notNullValue());
        assertThat(innerBuckets.size(), equalTo(4));
        for (int i = 0; i < innerBuckets.size(); i++) {
            Histogram.Bucket innerBucket = innerBuckets.get(i);
            if (i == 0) {
                assertThat(innerBucket.getAggregations().get("derivative"), nullValue());
            } else {
                assertThat(innerBucket.getAggregations().get("derivative"), notNullValue());
            }
        }
        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo("3.0"));
        innerHisto = bucket.getAggregations().get("inner_histo");
        assertThat(innerHisto, notNullValue());
        innerBuckets = innerHisto.getBuckets();
        assertThat(innerBuckets, notNullValue());
        assertThat(innerBuckets.size(), equalTo(4));
        for (int i = 0; i < innerBuckets.size(); i++) {
            Histogram.Bucket innerBucket = innerBuckets.get(i);
            if (i == 0) {
                assertThat(innerBucket.getAggregations().get("derivative"), nullValue());
            } else {
                assertThat(innerBucket.getAggregations().get("derivative"), notNullValue());
            }
        }
    }
}
