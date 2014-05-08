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
package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import org.apache.lucene.util.English;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

/**
 * Integration tests for benchmark API
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class BenchmarkIntegrationTest extends ElasticsearchIntegrationTest {

    private static final String BENCHMARK_NAME = "test_benchmark";
    private static final String COMPETITOR_PREFIX = "competitor_";
    private static final String INDEX_PREFIX = "test_index_";
    private static final String INDEX_TYPE = "test_type";

    private static final int MIN_DOC_COUNT = 1;
    private static final int MAX_DOC_COUNT = 1000;

    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private static final long TIMEOUT = 10;

    private int numExecutorNodes = 0;
    private Map<String, BenchmarkSettings> competitionSettingsMap;
    private String[] indices = Strings.EMPTY_ARRAY;

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put("node.bench", true).build();
    }

    @Before
    public void beforeBenchmarkIntegrationTests() throws Exception {
        numExecutorNodes = cluster().size();
        competitionSettingsMap = new HashMap<>();
        logger.info("--> indexing random data");
        indices = randomData();
    }

    @Test
    public void testSubmitBenchmark() throws Exception {

        final BenchmarkRequest request =
                BenchmarkTestUtil.randomRequest(client(),indices, numExecutorNodes, competitionSettingsMap);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());
        final BenchmarkResponse response = client().bench(request).actionGet();

        assertThat(response, notNullValue());
        assertThat(response.state(), equalTo(BenchmarkResponse.State.COMPLETE));
        assertFalse(response.hasErrors());
        assertThat(response.benchmarkName(), equalTo(BENCHMARK_NAME));
        assertThat(response.competitionResults().size(), equalTo(request.competitors().size()));

        for (CompetitionResult result : response.competitionResults().values()) {
            assertThat(result.nodeResults().size(), equalTo(numExecutorNodes));
            validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), true);
        }
    }

    @Test
    public void testListBenchmarks() throws Exception {

        final BenchmarkRequest request =
                BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());

        final CountDownLatch countdown = new CountDownLatch(1);
        final List<Throwable> throwables = new ArrayList<>();

        client().bench(request, new ActionListener<BenchmarkResponse>() {
            @Override
            public void onResponse(BenchmarkResponse benchmarkResponse) {
                countdown.countDown();
            }
            @Override
            public void onFailure(Throwable e) {
                throwables.add(e);
                countdown.countDown();
            }
        });

        // Attempt to 'yield' to the thread executing the benchmark; we want it to start executing, but not
        // to finish so that we can 'capture' an in-progress state
        Thread.sleep(1000);

        final BenchmarkStatusResponse response = client().prepareBenchStatus().execute().actionGet();
        assertThat(response.benchmarkResponses().size(), greaterThanOrEqualTo(0));

        for (BenchmarkResponse benchmarkResponse : response.benchmarkResponses()) {

            assertThat(benchmarkResponse.benchmarkName(), equalTo(BENCHMARK_NAME));
            assertThat(benchmarkResponse.state(), equalTo(BenchmarkResponse.State.RUNNING));
            assertFalse(benchmarkResponse.hasErrors());

            for (CompetitionResult result : benchmarkResponse.competitionResults().values()) {
                assertThat(result.nodeResults().size(), lessThanOrEqualTo(numExecutorNodes));
                validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), false);
            }
        }

        // Wait for active benchmark to finish; not fatal if we timeout as the framework will tear down the cluster
        if (!countdown.await(TIMEOUT, TIME_UNIT)) {
            logger.warn("Timeout waiting to for benchmark to complete");
        }

        if (throwables.size() > 0) {
            for (Throwable t : throwables) {
                logger.error(t.getMessage(), t);
            }
            fail("Failed to execute benchmark");
        }
    }

    @Test
    public void testAbortBenchmark() throws Exception {

        final BenchmarkRequest request =
                BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());

        final CountDownLatch countdown = new CountDownLatch(1);
        final List<Throwable> throwables = new ArrayList<>();

        client().bench(request, new ActionListener<BenchmarkResponse>() {
            @Override
            public void onResponse(BenchmarkResponse benchmarkResponse) {
                countdown.countDown();
                assertThat(benchmarkResponse.state(), equalTo(BenchmarkResponse.State.ABORTED));
            }
            @Override
            public void onFailure(Throwable e) {
                throwables.add(e);
                countdown.countDown();
            }
        });

        // Attempt to 'yield' to the thread executing the benchmark; we want it to start executing, but not
        // to finish so that we can successfully execute an abort operation on it.
        Thread.sleep(1000);

        final AbortBenchmarkResponse response = client().prepareAbortBench(BENCHMARK_NAME).execute().actionGet();
        assertThat(response.getNodeResponses().size(), lessThanOrEqualTo(numExecutorNodes));
        assertThat(response.getBenchmarkName(), equalTo(BENCHMARK_NAME));

        for (AbortBenchmarkNodeResponse nodeResponse : response.getNodeResponses()) {
            assertThat(nodeResponse.benchmarkName(), equalTo(BENCHMARK_NAME));
            assertThat(nodeResponse.errorMessage(), nullValue());
            assertThat(nodeResponse.nodeName(), notNullValue());
        }

        // Send a list request to make sure there are no active benchmarks
        final BenchmarkStatusResponse statusResponse = client().prepareBenchStatus().execute().actionGet();
        assertThat(statusResponse.benchmarkResponses().size(), equalTo(0));

        // Wait for active benchmark to finish; not fatal if we timeout as the framework will tear down the cluster
        if (!countdown.await(TIMEOUT, TIME_UNIT)) {
            logger.warn("Timeout waiting to for benchmark to complete");
        }

        if (throwables.size() > 0) {
            for (Throwable t : throwables) {
                logger.error(t.getMessage(), t);
            }
            fail("Failed to execute benchmark");
        }
    }

    @Test(expected = BenchmarkMissingException.class)
    public void testAbortNoSuchBenchmark() throws Exception {
        client().prepareAbortBench(BENCHMARK_NAME).execute().actionGet();
    }

    private void validateCompetitionResult(CompetitionResult result, BenchmarkSettings requestedSettings, boolean strict) {

        // Validate settings
        assertTrue(result.competitionName().startsWith(COMPETITOR_PREFIX));
        assertThat(result.concurrency(), equalTo(requestedSettings.concurrency()));
        assertThat(result.multiplier(), equalTo(requestedSettings.multiplier()));

        // Validate node-level responses
        for (CompetitionNodeResult nodeResult : result.nodeResults()) {

            assertThat(nodeResult.nodeName(), notNullValue());

            assertThat(nodeResult.totalIterations(), equalTo(requestedSettings.iterations()));
            if (strict) {
                assertThat(nodeResult.completedIterations(), equalTo(requestedSettings.iterations()));
                final int expectedQueryCount = requestedSettings.multiplier() *
                        nodeResult.totalIterations() * requestedSettings.searchRequests().size();
                assertThat(nodeResult.totalExecutedQueries(), equalTo(expectedQueryCount));
                assertThat(nodeResult.iterations().size(), equalTo(requestedSettings.iterations()));
            }

            assertThat(nodeResult.warmUpTime(), greaterThanOrEqualTo(0L));

            for (CompetitionIteration iteration : nodeResult.iterations()) {
                // Basic sanity checks
                iteration.computeStatistics();
                assertThat(iteration.totalTime(), greaterThanOrEqualTo(0L));
                assertThat(iteration.min(), greaterThanOrEqualTo(0L));
                assertThat(iteration.max(), greaterThanOrEqualTo(iteration.min()));
                assertThat(iteration.mean(), greaterThanOrEqualTo((double) iteration.min()));
                assertThat(iteration.mean(), lessThanOrEqualTo((double) iteration.max()));
                assertThat(iteration.queriesPerSecond(), greaterThanOrEqualTo(0.0));
                assertThat(iteration.millisPerHit(), greaterThanOrEqualTo(0.0));
                validatePercentiles(iteration.percentileValues());
            }
        }

        // Validate summary statistics
        final CompetitionSummary summary = result.competitionSummary();
        summary.computeSummaryStatistics();
        assertThat(summary, notNullValue());
        assertThat(summary.getMin(), greaterThanOrEqualTo(0L));
        assertThat(summary.getMax(), greaterThanOrEqualTo(summary.getMin()));
        assertThat(summary.getMean(), greaterThanOrEqualTo((double) summary.getMin()));
        assertThat(summary.getMean(), lessThanOrEqualTo((double) summary.getMax()));
        assertThat(summary.getTotalTime(), greaterThanOrEqualTo(0L));
        assertThat(summary.getQueriesPerSecond(), greaterThanOrEqualTo(0.0));
        assertThat(summary.getMillisPerHit(), greaterThanOrEqualTo(0.0));
        assertThat(summary.getAvgWarmupTime(), greaterThanOrEqualTo(0.0));
        if (strict) {
            assertThat((int) summary.getTotalIterations(), equalTo(requestedSettings.iterations() * summary.nodeResults().size()));
            assertThat((int) summary.getCompletedIterations(), equalTo(requestedSettings.iterations() * summary.nodeResults().size()));
            assertThat((int) summary.getTotalQueries(), equalTo(requestedSettings.iterations() * requestedSettings.multiplier() *
                    requestedSettings.searchRequests().size() * summary.nodeResults().size()));
            validatePercentiles(summary.percentileValues);
        }
    }

    private void validatePercentiles(Map<Double, Double> percentiles) {
        int i = 0;
        Double last = null;
        for (Map.Entry<Double, Double> entry : percentiles.entrySet()) {
            assertThat(entry.getKey(), equalTo(BenchmarkSettings.DEFAULT_PERCENTILES[i++]));
            if (last != null) {
                assertThat(entry.getValue(), greaterThanOrEqualTo(last));
            }
            // This is a hedge against rounding errors. Sometimes two adjacent percentile values will
            // be nearly equivalent except for some insignificant decimal places. In such cases we
            // want the two values to compare as equal.
            final BigDecimal bd = new BigDecimal(entry.getValue()).setScale(2, RoundingMode.HALF_DOWN);
            last = bd.doubleValue();
        }
    }

    private String[] randomData() throws Exception {

        final int numIndices = between(BenchmarkTestUtil.MIN_SMALL_INTERVAL, BenchmarkTestUtil.MAX_SMALL_INTERVAL);
        final String[] indices = new String[numIndices];

        for (int i = 0; i < numIndices; i++) {
            indices[i] = INDEX_PREFIX + i;
            final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
            final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

            for (int j = 0; j < numDocs; j++) {
                docs[j] = client().prepareIndex(indices[i], INDEX_TYPE).
                        setSource(BenchmarkTestUtil.TestIndexField.INT_FIELD.toString(), randomInt(),
                                  BenchmarkTestUtil.TestIndexField.FLOAT_FIELD.toString(), randomFloat(),
                                  BenchmarkTestUtil.TestIndexField.BOOLEAN_FIELD.toString(), randomBoolean(),
                                  BenchmarkTestUtil.TestIndexField.STRING_FIELD.toString(), English.intToEnglish(j));
            }

            indexRandom(true, docs);
        }

        return indices;
    }
}
