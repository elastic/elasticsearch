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
package org.elasticsearch.search.aggregations.metrics.stats.multifield;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.AbstractNumericTestCase;
import org.elasticsearch.search.aggregations.metrics.AvgIT;
import org.elasticsearch.search.aggregations.metrics.ValueCountIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.multifieldStats;
import static org.elasticsearch.test.hamcrest.DoubleMatcher.nearlyEqual;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
/**
 * MultiFieldStats aggregation integration test
 */
public class MultiFieldStatsIT extends AbstractNumericTestCase {
    protected static String[] valFieldNames = new String[] {"value", "randVal1", "randVal2", "values"};
    protected static String[] singleValFields = Arrays.copyOf(valFieldNames, 3);
    protected static MultiFieldStatsResults multiFieldStatsResults = MultiFieldStatsResults.EMPTY();
    protected static long numDocs = 10000L;
    protected static final double TOLERANCE = 1e-6;
    protected static final String aggName = "multifieldstats";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            AvgIT.ExtractFieldScriptPlugin.class,
            ValueCountIT.FieldValueScriptPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();
        createIndex("mfs_idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        RunningStats stats = new RunningStats();

        Map<String, Double> vals = new HashMap<>(3);
        for (int i = 0; i < numDocs; i++) {
            vals.put(valFieldNames[0], (double)i+1);
            vals.put(valFieldNames[1], randomDouble());
            vals.put(valFieldNames[2], randomDouble());
            vals.put(valFieldNames[3], ((i+2) + (i+3)) / 2.0);
            XContentBuilder source = jsonBuilder()
                .startObject()
                .field(valFieldNames[0], vals.get(valFieldNames[0]));
            boolean omit = true;
            // test random missing data
            if (randomBoolean() == true) {
                omit = false;
                source = source.field(valFieldNames[1], vals.get(valFieldNames[1]));
            }
            source = source.field(valFieldNames[2], vals.get(valFieldNames[2]))
                .startArray(valFieldNames[3]).value(i+2).value(i+3).endArray()
                .endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            // add document fields
            if (omit == false) {
                stats.add(vals);
            }
        }
        multiFieldStatsResults = new MultiFieldStatsResults(stats);
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                .subAggregation(multifieldStats(aggName).fields(Arrays.asList(singleValFields)))).execute().actionGet();
        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        assertEmptyMultiFieldStats(bucket.getAggregations().get(aggName));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName).fields(Arrays.asList(singleValFields)))
            .execute().actionGet();
        assertThat(response.getHits().getTotalHits(), equalTo(0L));

        assertEmptyMultiFieldStats(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(singleValFields)))
            .execute().actionGet();
        assertSearchResponse(response);

        assertHitCount(response, numDocs);

        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(multifieldStats(aggName).fields(Arrays.asList(singleValFields))))
            .execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);

        Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        assertExpectedStatsResults((MultiFieldStats)global.getProperty(aggName));
        // check values by using property paths
        assertExpectedStatsResultsByProperty((MultiFieldStats)global.getProperty(aggName));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped").setQuery(matchAllQuery())
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(singleValFields))).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);

        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("value", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, null));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(singleValFields)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "value");
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, params));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(singleValFields)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldNames)))
            .execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);

        assertExpectedStatsResults(response.getAggregations().get(aggName), valFieldNames);
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("values", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, null));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(valFieldNames)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName), valFieldNames);
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "values");
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, params));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(valFieldNames)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName), valFieldNames);
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        HashMap<String, Script> scripts = new HashMap<>();
        scripts.put("extract_field", new Script("value", ScriptType.INLINE, AvgIT.ExtractFieldScriptEngine.NAME, Collections.EMPTY_MAP));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(singleValFields)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "value");
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, params));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(singleValFields)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        HashMap<String, Script> scripts = new HashMap<>();
        scripts.put("extract_field", new Script("values", ScriptType.INLINE, AvgIT.ExtractFieldScriptEngine.NAME, Collections.EMPTY_MAP));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(singleValFields)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "values");
        Map<String, Script> scripts = Collections.singletonMap("field_value",
            new Script("", ScriptType.INLINE, ValueCountIT.FieldValueScriptEngine.NAME, params));
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName)
            .fields(Arrays.asList(valFieldNames)).scripts(scripts)).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);
        assertExpectedStatsResults(response.getAggregations().get(aggName), valFieldNames);
    }


    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        // TODO implement
    }

    private void assertEmptyMultiFieldStats(MultiFieldStats multiFieldStats) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));
        assertThat(multiFieldStats.getFieldCount("value"), equalTo(0L));
        assertNull(multiFieldStats.getMean("value"));
        assertNull(multiFieldStats.getVariance("value"));
        assertNull(multiFieldStats.getSkewness("value"));
        assertNull(multiFieldStats.getKurtosis("value"));
        assertNull(multiFieldStats.getCovariance("value", "randVal1"));
        assertNull(multiFieldStats.getCorrelation("value", "randVal1"));
    }

    private void assertExpectedStatsResults(MultiFieldStats multiFieldStats) {
        assertExpectedStatsResults(multiFieldStats, singleValFields);
    }

    private void assertExpectedStatsResults(MultiFieldStats multiFieldStats, String[] fieldNames) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));
        for (String fieldName : fieldNames) {
            // check fieldCount
            assertThat(multiFieldStats.getFieldCount(fieldName), equalTo(multiFieldStatsResults.getFieldCount(fieldName)));
            // check mean
            nearlyEqual(multiFieldStats.getMean(fieldName), multiFieldStatsResults.getMean(fieldName), TOLERANCE);
            // check variance
            nearlyEqual(multiFieldStats.getVariance(fieldName), multiFieldStatsResults.getVariance(fieldName), TOLERANCE);
            // check skewness
            nearlyEqual(multiFieldStats.getSkewness(fieldName), multiFieldStatsResults.getSkewness(fieldName), TOLERANCE);
            // check kurtosis
            nearlyEqual(multiFieldStats.getKurtosis(fieldName), multiFieldStatsResults.getKurtosis(fieldName), TOLERANCE);
            // check covariance
            for (String fieldNameY : fieldNames) {
                // check covariance
                nearlyEqual(multiFieldStats.getCovariance(fieldName, fieldNameY),
                    multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE);
                // check correlation
                nearlyEqual(multiFieldStats.getCorrelation(fieldName, fieldNameY),
                    multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE);
            }
        }
    }

    private void assertExpectedStatsResultsByProperty(MultiFieldStats multiFieldStats) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));

        Map<String, Double> counts = (Map<String, Double>) multiFieldStats.getProperty("counts");
        Map<String, Double> means = (Map<String, Double>) multiFieldStats.getProperty("means");
        Map<String, Double> variances = (Map<String, Double>) multiFieldStats.getProperty("variances");
        Map<String, Double> skewness = (Map<String, Double>) multiFieldStats.getProperty("skewness");
        Map<String, Double> kurtosis = (Map<String, Double>) multiFieldStats.getProperty("kurtosis");
        Map<String, HashMap<String, Double>> covariance =
            (Map<String, HashMap<String, Double>>) multiFieldStats.getProperty("covariance");
        Map<String, HashMap<String, Double>> correlation =
            (Map<String, HashMap<String, Double>>) multiFieldStats.getProperty("correlation");

        for (String fieldName : singleValFields) {
            // check fieldCount
            assertThat(counts.get(fieldName), equalTo(multiFieldStatsResults.getFieldCount(fieldName)));
            // check mean
            nearlyEqual(means.get(fieldName), multiFieldStatsResults.getMean(fieldName), TOLERANCE);
            // check variance
            nearlyEqual(variances.get(fieldName), multiFieldStatsResults.getVariance(fieldName), TOLERANCE);
            // check skewness
            nearlyEqual(skewness.get(fieldName), multiFieldStatsResults.getSkewness(fieldName), TOLERANCE);
            // check kurtosis
            nearlyEqual(kurtosis.get(fieldName), multiFieldStatsResults.getKurtosis(fieldName), TOLERANCE);
            // check covariance and correlation
            HashMap<String, Double> colCov;
            HashMap<String, Double> colCorr;
            for (String fieldNameY : singleValFields) {
                // if the fieldnames are the same just return the variance
                if (fieldName.equals(fieldNameY)) {
                    nearlyEqual(variances.get(fieldName),
                        multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE);
                    nearlyEqual(1.0D,
                        multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE);
                } else {
                    // the x and y field names may be swapped when pulled from the aggregation, so we have this
                    // irritating flip flop logic
                    colCov = null;
                    colCorr = null;
                    if (covariance.containsKey(fieldName)) {
                        colCov = covariance.get(fieldName);
                    }
                    if (correlation.containsKey(fieldName)) {
                        colCorr = correlation.get(fieldName);
                    }

                    String fnY = fieldNameY;
                    if (colCov == null || colCov.containsKey(fnY) == false) {
                        colCov = covariance.get(fieldNameY);
                        fnY = fieldName;
                    }
                    // check covariance
                    nearlyEqual(colCov.get(fnY),
                        multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE);

                    if (colCorr == null || colCorr.containsKey(fnY) == false) {
                        colCorr = correlation.get(fieldNameY);
                        fnY = fieldName;
                    }

                    // check correlation
                    nearlyEqual(colCorr.get(fnY),
                        multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE);
                }
            }
        }
    }
}
