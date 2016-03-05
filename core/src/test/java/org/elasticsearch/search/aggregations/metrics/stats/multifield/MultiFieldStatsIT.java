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
import org.elasticsearch.search.aggregations.metrics.AbstractNumericTestCase;

import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.multifieldStats;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

/**
 * MultiFieldStats aggregation integration test
 * todo: refactor to a general multi_stats aggregation?
 */
public class MultiFieldStatsIT extends AbstractNumericTestCase {
    protected static String[] valFieldName = new String[] {"value", "randVal1", "randVal2"};
    protected static MultiFieldStatsResults multiFieldStatsResults = new MultiFieldStatsResults();
    protected static int numDocs = 10000;
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
        //
    }

    @Override
    public void testUnmapped() throws Exception {

    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(multifieldStats(aggName).fields(Arrays.asList(valFieldName)))
            .execute().actionGet();
        assertSearchResponse(response);

        assertHitCount(response, numDocs);
        MultiFieldStats multiFieldStats = response.getAggregations().get(aggName);
        assertThat(multiFieldStats, notNullValue());
        assertThat(multiFieldStats.getName(), equalTo(aggName));
        // check mean
        for (String fieldName : valFieldName) {
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

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {

    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {

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
}
