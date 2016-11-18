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
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.bucketScript;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketScriptTests extends ESIntegTestCase {

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
        return pluginList(GroovyPlugin.class);
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

        client().preparePutIndexedScript().setId("my_script").setScriptLang(GroovyScriptEngineService.NAME).setSource("{ \"script\": \"_value0 + _value1 + _value2\" }").get();

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

    @Test
    public void inlineScript() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("_value0 + _value1 + _value2", ScriptType.INLINE, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    @Test
    public void inlineScript2() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("_value0 + _value1 / _value2", ScriptType.INLINE, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue / field4SumValue));
            }
        }
    }

    @Test
    public void inlineScriptWithDateRange() throws Exception {
        client().admin().indices().prepareCreate("date_range_index").addMapping("type",
                "{ \"type\": { \"properties\": { \"" + FIELD_4_NAME + "\": { \"type\": \"date\" } } } }").get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < 4; docs++) {
            XContentBuilder jsonBuilder = jsonBuilder();
            jsonBuilder.startObject();
            jsonBuilder.field(FIELD_1_NAME, docs + 1);
            jsonBuilder.field(FIELD_2_NAME, docs + 1);
            jsonBuilder.field(FIELD_3_NAME, docs + 2);
            jsonBuilder.field(FIELD_4_NAME, "2016-11-0" + (docs + 1));
            jsonBuilder.endObject();
            builders.add(client().prepareIndex("date_range_index", "type").setSource(jsonBuilder));
        }

        indexRandom(true, builders);
        ensureSearchable();

        SearchResponse response = client().prepareSearch("date_range_index")
                .addAggregation(dateRange("range").field(FIELD_4_NAME).addRange(null, "2016-11-03").addRange("2016-11-03", null)
                        .subAggregation(avg("b").field(FIELD_2_NAME)).subAggregation(avg("c").field(FIELD_3_NAME))
                        .subAggregation(bucketScript("diff").setBucketsPaths("b", "c")
                                .script(new Script("_value1 - _value0", ScriptType.INLINE, null, null))))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalDateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<InternalDateRange.Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        InternalDateRange.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getFrom(), nullValue());
        assertThat(bucket.getTo(), equalTo((Object) new DateTime(2016, 11, 03, 0, 0, 0, 0, DateTimeZone.UTC)));
        SimpleValue diff = bucket.getAggregations().get("diff");
        assertThat(diff, notNullValue());
        double diffValue = diff.value();
        assertThat(diffValue, equalTo(1.0));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getFrom(), equalTo((Object) new DateTime(2016, 11, 03, 0, 0, 0, 0, DateTimeZone.UTC)));
        assertThat(bucket.getTo(), nullValue());
        diff = bucket.getAggregations().get("diff");
        assertThat(diff, notNullValue());
        diffValue = diff.value();
        assertThat(diffValue, equalTo(1.0));
    }

    @Test
    public void inlineScriptSingleVariable() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum").script(
                                                new Script("_value0", ScriptType.INLINE, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue));
            }
        }
    }

    @Test
    public void inlineScriptNamedVars() {

        Map<String, String> bucketsPathsMap = new HashMap<>();
        bucketsPathsMap.put("foo", "field2Sum");
        bucketsPathsMap.put("bar", "field3Sum");
        bucketsPathsMap.put("baz", "field4Sum");
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPathsMap(bucketsPathsMap ).script(
                                                new Script("foo + bar + baz", ScriptType.INLINE, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    @Test
    public void inlineScriptWithParams() {

        Map<String, Object> params = new HashMap<>();
        params.put("factor", 3);
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("(_value0 + _value1 + _value2) * factor", ScriptType.INLINE, null, params)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo((field2SumValue + field3SumValue + field4SumValue) * 3));
            }
        }
    }

    @Test
    public void inlineScriptInsertZeros() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("_value0 + _value1 + _value2", ScriptType.INLINE, null, null)).gapPolicy(GapPolicy.INSERT_ZEROS))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

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
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    @Test
    public void indexedScript() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("my_script", ScriptType.INDEXED, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("_value0 + _value1 + _value2", ScriptType.INLINE, null, null))))
                                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic").setBucketsPaths("field2Sum", "field3Sum", "field4Sum").script(
                                                new Script("_value0 + _value1 + _value2", ScriptType.INLINE, null, null)))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }
}
