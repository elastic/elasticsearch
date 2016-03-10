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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.AbstractNumericTestCase;

import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.multifieldStats;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

/**
 * MultiFieldStats aggregation integration test
 */
public class MultiFieldStatsIT extends AbstractNumericTestCase {
    protected static String[] valFieldName = new String[] {"value", "randVal1", "randVal2"};
    protected static MultiFieldStatsResults multiFieldStatsResults = new MultiFieldStatsResults();
    protected static long numDocs = 10000L;
    protected static final double TOLERANCE = 1e-6;
    protected static final String aggName = "multifieldstats";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();
        createIndex("mfs_idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        Map<String, Double> vals = new HashMap<>(3);
        for (int i = 0; i < numDocs; i++) {
            vals.put(valFieldName[0], (double)i+1);
            vals.put(valFieldName[1], randomDouble());
            vals.put(valFieldName[2], randomDouble());
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                .startObject()
                .field(valFieldName[0], vals.get(valFieldName[0]))
                .field(valFieldName[1], vals.get(valFieldName[1]))
                .field(valFieldName[2], vals.get(valFieldName[2]))
                .startArray("values").value(i+2).value(i+3).endArray()
                .endObject()));
            // add document fields
            multiFieldStatsResults.add(vals);
        }
        multiFieldStatsResults.computeStats();
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                .subAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName)))).execute().actionGet();
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
            .setQuery(matchAllQuery()).addAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName)))
            .execute().actionGet();
        assertThat(response.getHits().getTotalHits(), equalTo(0L));

        assertEmptyMultiFieldStats(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName)))
            .execute().actionGet();
        assertSearchResponse(response);

        assertHitCount(response, numDocs);

        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName)))).execute().actionGet();
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
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName))).execute().actionGet();
        assertSearchResponse(response);
        assertHitCount(response, numDocs);

        assertExpectedStatsResults(response.getAggregations().get(aggName));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {

    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {

    }

    @Override
    public void testMultiValuedField() throws Exception {

    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {

    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {

    }

    @Override
    public void testScriptSingleValued() throws Exception {

    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {

    }

    @Override
    public void testScriptMultiValued() throws Exception {

    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {

    }

    private void assertEmptyMultiFieldStats(MultiFieldStats multiFieldStats) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));
        assertThat(multiFieldStats.getFieldCount("value"), equalTo(0L));
        assertThat(Double.isNaN(multiFieldStats.getMean("value")), is(true));
        assertThat(Double.isNaN(multiFieldStats.getVariance("value")), is(true));
        assertThat(Double.isNaN(multiFieldStats.getSkewness("value")), is(true));
        assertThat(Double.isNaN(multiFieldStats.getKurtosis("value")), is(true));
        assertThat(Double.isNaN(multiFieldStats.getCovariance("value", "randVal1")), is(true));
        assertThat(Double.isNaN(multiFieldStats.getCorrelation("value", "randVal1")), is(true));
    }

    private void assertExpectedStatsResults(MultiFieldStats multiFieldStats) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));
        for (String fieldName : valFieldName) {
            // check fieldCount
            assertThat(multiFieldStats.getFieldCount(fieldName), equalTo(multiFieldStatsResults.counts.get(fieldName)));
            // check mean
            assertThat(multiFieldStats.getMean(fieldName), closeTo(multiFieldStatsResults.means.get(fieldName), TOLERANCE));
            // check variance
            assertThat(multiFieldStats.getVariance(fieldName), closeTo(multiFieldStatsResults.variances.get(fieldName), TOLERANCE));
            // check skewness
            assertThat(multiFieldStats.getSkewness(fieldName), closeTo(multiFieldStatsResults.skewness.get(fieldName), TOLERANCE));
            // check kurtosis
            assertThat(multiFieldStats.getKurtosis(fieldName), closeTo(multiFieldStatsResults.kurtosis.get(fieldName), TOLERANCE));
            // check covariance
            for (String fieldNameY : valFieldName) {
                // check covariance
                assertThat(multiFieldStats.getCovariance(fieldName, fieldNameY),
                    closeTo(multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE));
                // check correlation
                assertThat(multiFieldStats.getCorrelation(fieldName, fieldNameY),
                    closeTo(multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE));
            }
        }
    }

    private void assertExpectedStatsResultsByProperty(MultiFieldStats multiFieldStats) {
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));

        HashMap<String, Double> counts = (HashMap<String, Double>) multiFieldStats.getProperty("counts");
        HashMap<String, Double> means = (HashMap<String, Double>) multiFieldStats.getProperty("means");
        HashMap<String, Double> variances = (HashMap<String, Double>) multiFieldStats.getProperty("variances");
        HashMap<String, Double> skewness = (HashMap<String, Double>) multiFieldStats.getProperty("skewness");
        HashMap<String, Double> kurtosis = (HashMap<String, Double>) multiFieldStats.getProperty("kurtosis");
        HashMap<String, HashMap<String, Double>> covariance =
            (HashMap<String, HashMap<String, Double>>) multiFieldStats.getProperty("covariance");
        HashMap<String, HashMap<String, Double>> correlation =
            (HashMap<String, HashMap<String, Double>>) multiFieldStats.getProperty("correlation");

        for (String fieldName : valFieldName) {
            // check fieldCount
            assertThat(counts.get(fieldName), equalTo(multiFieldStatsResults.counts.get(fieldName)));
            // check mean
            assertThat(means.get(fieldName), closeTo(multiFieldStatsResults.means.get(fieldName), TOLERANCE));
            // check variance
            assertThat(variances.get(fieldName), closeTo(multiFieldStatsResults.variances.get(fieldName), TOLERANCE));
            // check skewness
            assertThat(skewness.get(fieldName), closeTo(multiFieldStatsResults.skewness.get(fieldName), TOLERANCE));
            // check kurtosis
            assertThat(kurtosis.get(fieldName), closeTo(multiFieldStatsResults.kurtosis.get(fieldName), TOLERANCE));
            // check covariance and correlation
            HashMap<String, Double> colCov;
            HashMap<String, Double> colCorr;
            for (String fieldNameY : valFieldName) {
                // if the fieldnames are the same just return the variance
                if (fieldName.equals(fieldNameY)) {
                    assertThat(variances.get(fieldName),
                        closeTo(multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE));
                    assertThat(1.0D,
                        closeTo(multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE));
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
                    assertThat(colCov.get(fnY),
                        closeTo(multiFieldStatsResults.getCovariance(fieldName, fieldNameY), TOLERANCE));

                    if (colCorr == null || colCorr.containsKey(fnY) == false) {
                        colCorr = correlation.get(fieldNameY);
                        fnY = fieldName;
                    }

                    // check correlation
                    assertThat(colCorr.get(fnY),
                        closeTo(multiFieldStatsResults.getCorrelation(fieldName, fieldNameY), TOLERANCE));
                }
            }
        }
    }
}
