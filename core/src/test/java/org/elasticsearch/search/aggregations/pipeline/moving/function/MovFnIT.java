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

package org.elasticsearch.search.aggregations.pipeline.moving.function;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregationHelperTests;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MaxModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MedianModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MinModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.SumModel;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.test.hamcrest.DoubleMatcher.nearlyEqual;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.movingFunction;

@ESIntegTestCase.SuiteScopeTestCase
public class MovFnIT extends ESIntegTestCase {

    private static final String INTERVAL_FIELD = "l_value";
    private static final String VALUE_FIELD = "v_value";

    static int interval;
    static int numBuckets;
    static int windowSize;
    static BucketHelpers.GapPolicy gapPolicy;
    static List<PipelineAggregationHelperTests.MockBucket> mockHisto;
    static ValuesSourceAggregationBuilder<? extends ValuesSource, ? extends ValuesSourceAggregationBuilder<?, ?>> metric;
    static Map<String, ArrayList<Double>> testValues;


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("return 1;", vars -> 1);
            return scripts;
        }
    }

    enum MovFnType {
        MIN("min"), MAX("max"), MEDIAN("median"), SUM("sum"), SCRIPT("script");

        private final String name;

        MovFnType(String s) {
            name = s;
        }

        @Override
        public String toString() {
            return name;
        }
    }


    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        interval = 5;
        numBuckets = randomIntBetween(6, 10);
        windowSize = randomIntBetween(2, 4);
        metric = randomMetric("the_metric", VALUE_FIELD);
        gapPolicy = randomBoolean() ? BucketHelpers.GapPolicy.SKIP : BucketHelpers.GapPolicy.INSERT_ZEROS;
        mockHisto = PipelineAggregationHelperTests.generateHistogram(interval, numBuckets, randomDouble(), randomDouble());

        testValues = new HashMap<>(8);

        for (MovFnType type : MovFnType.values()) {
            setupExpected(type, windowSize);
        }

        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            for (double value : mockBucket.docValues) {
                builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                    .field(INTERVAL_FIELD, mockBucket.key)
                    .field(VALUE_FIELD, value).endObject()));
            }
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    /**
     * Calculates the moving fn for a specific model based on the previously generated mock histogram.
     * Computed values are stored in the testValues map.
     *
     * @param type The moving average model to use
     */
    private void setupExpected(MovFnType type, int windowSize) {
        ArrayList<Double> values = new ArrayList<>(numBuckets);
        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);

        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            double metricValue;
            double[] docValues = mockBucket.docValues;

            if (mockBucket.count == 0) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.SKIP)) {
                    values.add(null);
                    continue;
                } else {
                    // otherwise insert a zero instead of the true value
                    metricValue = 0.0;
                }

            } else {
                // If this isn't a gap, just insert the value
                metricValue = PipelineAggregationHelperTests.calculateMetric(docValues, metric);
            }

            if (window.size() > 0) {
                switch (type) {
                    case MIN:
                        values.add(window.stream().mapToDouble(v -> v).min().orElse(Double.NaN));
                        break;
                    case MAX:
                        values.add(window.stream().mapToDouble(v -> v).max().orElse(Double.NaN));
                        break;
                    case SUM:
                        values.add(window.stream().mapToDouble(v -> v).sum());
                        break;
                    case MEDIAN:
                        values.add(median(window));
                        break;
                    case SCRIPT:
                        values.add(1.0);
                        break;
                }
            } else {
                values.add(null);
            }

            window.offer(metricValue);

        }
        testValues.put(type.name(), values);
    }

    private double median(Collection<Double> window) {
        List<Double> listValues = new ArrayList<>(window);
        Collections.sort(listValues);
        if (window.size() % 2 == 0) {
            int middle = (int) Math.floor(window.size() / 2.0);
            return (listValues.get(middle - 1) + listValues.get(middle)) / 2;
        }

        return listValues.get(listValues.size() / 2);
    }

    public void testMin() {
        SearchResponse response = client()
            .prepareSearch("idx").setTypes("type")
            .addAggregation(
                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                    .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                    .subAggregation(metric)
                    .subAggregation(movingFunction("movfn_values", "the_metric", null)
                        .window(windowSize)
                        .modelBuilder(new MinModel.MinModelBuilder())
                        .gapPolicy(gapPolicy))
            ).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedValues = testValues.get(MovFnType.MIN.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long) expected.count));

            assertBucketContents(actual, expectedValue);
        }
    }

    public void testMax() {
        SearchResponse response = client()
            .prepareSearch("idx").setTypes("type")
            .addAggregation(
                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                    .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                    .subAggregation(metric)
                    .subAggregation(movingFunction("movfn_values", "the_metric", null)
                        .window(windowSize)
                        .modelBuilder(new MaxModel.MaxModelBuilder())
                        .gapPolicy(gapPolicy))
            ).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedValues = testValues.get(MovFnType.MAX.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long) expected.count));

            assertBucketContents(actual, expectedValue);
        }
    }

    public void testMedian() {
        SearchResponse response = client()
            .prepareSearch("idx").setTypes("type")
            .addAggregation(
                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                    .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                    .subAggregation(metric)
                    .subAggregation(movingFunction("movfn_values", "the_metric", null)
                        .window(windowSize)
                        .modelBuilder(new MedianModel.MedianModelBuilder())
                        .gapPolicy(gapPolicy))
            ).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedValues = testValues.get(MovFnType.MEDIAN.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long) expected.count));

            assertBucketContents(actual, expectedValue);
        }
    }

    public void testSum() {
        SearchResponse response = client()
            .prepareSearch("idx").setTypes("type")
            .addAggregation(
                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                    .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                    .subAggregation(metric)
                    .subAggregation(movingFunction("movfn_values", "the_metric", null)
                        .window(windowSize)
                        .modelBuilder(new SumModel.MinModelBuilder())
                        .gapPolicy(gapPolicy))
            ).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedValues = testValues.get(MovFnType.SUM.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long) expected.count));

            assertBucketContents(actual, expectedValue);
        }
    }

    public void testScript() {
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return 1;", Collections.emptyMap());
        SearchResponse response = client()
            .prepareSearch("idx").setTypes("type")
            .addAggregation(
                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                    .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                    .subAggregation(metric)
                    .subAggregation(movingFunction("movfn_values", "the_metric", null)
                        .window(windowSize)
                        .setScript(script)
                        .gapPolicy(gapPolicy))
            ).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedValues = testValues.get(MovFnType.SCRIPT.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long) expected.count));

            assertBucketContents(actual, expectedValue);
        }
    }

    private void assertValidIterators(Iterator expectedBucketIter, Iterator expectedValuesIter) {
        if (!expectedBucketIter.hasNext()) {
            fail("`expectedBucketIter` iterator ended before `actual` iterator, size mismatch");
        }
        if (!expectedValuesIter.hasNext()) {
            fail("`expectedValuesIter` iterator ended before `actual` iterator, size mismatch");
        }
    }

    private void assertBucketContents(Histogram.Bucket actual, Double expectedValue) {

        SimpleValue values = actual.getAggregations().get("movfn_values");
        if (expectedValue == null) {
            assertThat("[value] moving is not null", values, Matchers.nullValue());
        } else if (Double.isNaN(expectedValue)) {
            assertThat("[value] moving should be NaN, but is [" + values.value() + "] instead",
                values.value(), Matchers.equalTo(Double.NaN));
        } else {
            assertThat("[value] moving is null", values, IsNull.notNullValue());
            assertTrue("[value] moving does not match expected [" + values.value() + " vs " + expectedValue + "]",
                nearlyEqual(values.value(), expectedValue, 0.1));
        }
    }

    private static ValuesSourceAggregationBuilder<? extends ValuesSource,
        ? extends ValuesSourceAggregationBuilder<?, ?>> randomMetric(String name, String field) {
        int rand = randomIntBetween(0, 3);

        switch (rand) {
            case 0:
                return min(name).field(field);
            case 2:
                return max(name).field(field);
            case 3:
                return avg(name).field(field);
            default:
                return avg(name).field(field);
        }
    }
}
