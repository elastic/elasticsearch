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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.having;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketSelectorTests extends ESIntegTestCase {

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
        return Collections.singleton(GroovyPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = randomIntBetween(1, 50);
        numDocs = randomIntBetween(10, 500);
        minNumber = -200;
        maxNumber = 200;

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < numDocs; docs++) {
            builders.add(client().prepareIndex("idx", "type").setSource(newDocBuilder()));
        }

        client().preparePutIndexedScript().setId("my_script").setScriptLang(GroovyScriptEngineService.NAME)
                .setSource("{ \"script\": \"Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)\" }").get();

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
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScript() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)", ScriptType.INLINE,
                                                        null, null)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? true : (_value0 < 10000)", ScriptType.INLINE, null,
                                                        null)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 > 10000)", ScriptType.INLINE, null,
                                                        null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }

    public void testInlineScript2() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 < _value1)", ScriptType.INLINE, null,
                                                        null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum")
                                                .script(new Script("Double.isNaN(_value0) ? false : (_value0 > 100)", ScriptType.INLINE,
                                                        null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        Map<String, String> bucketPathsMap = new HashMap<>();
        bucketPathsMap.put("my_value1", "field2Sum");
        bucketPathsMap.put("my_value2", "field3Sum");

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPathsMap(bucketPathsMap).script(
                                                new Script("Double.isNaN(my_value1) ? false : (my_value1 + my_value2 > 100)",
                                                        ScriptType.INLINE, null, null)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        Map<String, Object> params = new HashMap<>();
        params.put("threshold", 100);
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 + _value1 > threshold)",
                                                        ScriptType.INLINE, null, params)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").gapPolicy(GapPolicy.INSERT_ZEROS)
                                                .script(new Script("_value0 + _value1 > 100", ScriptType.INLINE, null, null))))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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

    public void testIndexedScript() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("my_script", ScriptType.INDEXED, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)", ScriptType.INLINE,
                                                        null, null)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(
                                        having("having").setBucketsPaths("field2Sum", "field3Sum").script(
                                                new Script("Double.isNaN(_value0) ? false : (_value0 + _value1 > 100)", ScriptType.INLINE,
                                                        null, null)))).execute()
                .actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
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
}
