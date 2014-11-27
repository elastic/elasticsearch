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

import com.google.common.base.Predicate;
import org.apache.lucene.util.English;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for benchmark API
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
@Ignore
public class BenchmarkIntegrationTest extends ElasticsearchIntegrationTest {

    private static final String BENCHMARK_NAME = "test_benchmark";
    private static final String BENCHMARK_NAME_WILDCARD = "test_*";
    private static final String COMPETITOR_PREFIX = "competitor_";
    private static final String INDEX_PREFIX = "test_index_";
    private static final String INDEX_TYPE = "test_type";

    private int numExecutorNodes = 0;
    private Map<String, BenchmarkSettings> competitionSettingsMap;
    private String[] indices = Strings.EMPTY_ARRAY;
    private HashMap<Integer, Boolean> benchNodes = new HashMap<>();



    protected synchronized Settings nodeSettings(int nodeOrdinal) {
        if (nodeOrdinal == 0) { // at least one
            return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                    .put("node.bench", true).put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false).build();
        } else {
            if (benchNodes.containsKey(nodeOrdinal)) {
                return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                        .put("node.bench", benchNodes.get(nodeOrdinal)).put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false).build();
            } else {
                boolean b = randomBoolean();
                benchNodes.put(nodeOrdinal, b);
                return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                        .put("node.bench", b).put(GroovyScriptEngineService.GROOVY_SCRIPT_SANDBOX_ENABLED, false).build();
            }
        }
    }

    @After
    public void afterBenchmarkIntegrationTests() throws Exception {
        final BenchmarkStatusResponse statusResponse = client().prepareBenchStatus().execute().actionGet();
        assertThat("Some benchmarks are still running", statusResponse.benchmarkResponses(), is(empty()));
    }

    @Before
    public void beforeBenchmarkIntegrationTests() throws Exception {
        waitForTestLatch = null;
        waitForQuery = null;
        numExecutorNodes = internalCluster().numBenchNodes();
        competitionSettingsMap = new HashMap<>();
        logger.info("--> indexing random data");
        indices = randomData();
    }

    @Test
    public void testSubmitBenchmark() throws Exception {
        final int iters = between(1, 3); // we run this more than once to make sure metadata is cleaned up propperly
        for (int i = 0; i < iters ; i++) {
            final BenchmarkRequest request =
                    BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);
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
    }

    @Test
    public void testListBenchmarks() throws Exception {
        SearchRequest searchRequest = prepareBlockingScriptQuery();
        final BenchmarkRequest request =
                BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap, searchRequest);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());

        final ActionFuture<BenchmarkResponse> future = client().bench(request);
        try {
            waitForQuery.await();
            final BenchmarkStatusResponse statusResponse = client().prepareBenchStatus().execute().actionGet();
            waitForTestLatch.countDown();
            assertThat(statusResponse.benchmarkResponses().size(), equalTo(1));
            for (BenchmarkResponse benchmarkResponse : statusResponse.benchmarkResponses()) {
                assertThat(benchmarkResponse.benchmarkName(), equalTo(BENCHMARK_NAME));
                assertThat(benchmarkResponse.state(), equalTo(BenchmarkResponse.State.RUNNING));
                assertFalse(benchmarkResponse.hasErrors());

                for (CompetitionResult result : benchmarkResponse.competitionResults().values()) {
                    assertThat(result.nodeResults().size(), lessThanOrEqualTo(numExecutorNodes));
                    validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), false);
                }
            }

        } finally {
            if (waitForTestLatch.getCount() == 1) {
                waitForTestLatch.countDown();
            }
            client().prepareAbortBench(BENCHMARK_NAME).get();
            // Confirm that there are no active benchmarks in the cluster
            assertThat(client().prepareBenchStatus().execute().actionGet().totalActiveBenchmarks(), equalTo(0));
            assertThat(waitForTestLatch.getCount(), is(0l));
        }
        // Confirm that benchmark was indeed aborted
        assertThat(future.get().state(), isOneOf(BenchmarkResponse.State.ABORTED, BenchmarkResponse.State.COMPLETE));

    }

    public static CountDownLatch waitForTestLatch;
    public static CountDownLatch waitForQuery;

    private SearchRequest prepareBlockingScriptQuery() {
        /* Chuck Norris back in the house!! - this is super evil but the only way at this
           point to ensure we actually call abort / list while a benchmark is executing
           without doing busy waiting etc. This Script calls the two static latches above and this test
           will not work if somebody messes around with them but it's much faster and less resource intensive / hardware
           dependent to run massive benchmarks and do busy waiting. */
        internalCluster(); // mark that we need a JVM local cluster!
        waitForQuery = new CountDownLatch(1);
        waitForTestLatch = new CountDownLatch(1);
        String className = "BenchmarkIntegrationTest";
        ScriptScoreFunctionBuilder scriptFunction = scriptFunction("import " + this.getClass().getName() + "; \n" +
                className + ".waitForQuery.countDown(); \n" + className + ".waitForTestLatch.await(); \n return 1.0;");
        SearchRequest searchRequest = searchRequest().source(
                searchSource()
                        .query(functionScoreQuery(FilterBuilders.matchAllFilter(), scriptFunction)));
        return  searchRequest;
    }

    @Test
    public void testBenchmarkWithErrors() {
        List<SearchRequest> reqList = new ArrayList<>();
        int numQueries = scaledRandomIntBetween(20, 100);
        int numErrors = scaledRandomIntBetween(1, numQueries);
        final boolean containsFatal = randomBoolean();
        if (containsFatal) {
            ScriptScoreFunctionBuilder scriptFunction = scriptFunction("DOES NOT COMPILE - fails on any shard");
            SearchRequest searchRequest = searchRequest().source(
                    searchSource()
                            .query(functionScoreQuery(FilterBuilders.matchAllFilter(), scriptFunction)));
            reqList.add(searchRequest);

        }
        for (int i = 0; reqList.size() < numErrors; i++) {
            ScriptScoreFunctionBuilder scriptFunction = scriptFunction("throw new RuntimeException();");
            SearchRequest searchRequest = searchRequest().source(
                    searchSource()
                            .query(functionScoreQuery(FilterBuilders.matchAllFilter(), scriptFunction)));
            reqList.add(searchRequest);
        }
        logger.info("--> run with [{}] errors ", numErrors);
        for (int i = 0; reqList.size() < numQueries; i++) {

            reqList.add(BenchmarkTestUtil.randomSearch(client(), indices));
        }
        Collections.shuffle(reqList, getRandom());

        final BenchmarkRequest request =
                BenchmarkTestUtil.randomRequest(client(),indices, numExecutorNodes, competitionSettingsMap, reqList.toArray(new SearchRequest[0]));
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());
        final BenchmarkResponse response = client().bench(request).actionGet();

        assertThat(response, notNullValue());
        if (response.hasErrors() || containsFatal) {
            assertThat(response.state(), equalTo(BenchmarkResponse.State.FAILED));
        } else {
            assertThat(response.state(), equalTo(BenchmarkResponse.State.COMPLETE));
            for (CompetitionResult result : response.competitionResults().values()) {
                assertThat(result.nodeResults().size(), equalTo(numExecutorNodes));
                validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), true);
            }
        }
        assertThat(response.benchmarkName(), equalTo(BENCHMARK_NAME));
    }

    @Test
    public void testAbortByPattern() throws Exception {
        final int iters = between(1, 3); // we run this more than once to make sure metadata is cleaned up propperly
        for (int i = 0; i < iters ; i++) {
            List<BenchmarkRequest> requests = new ArrayList<>();
            List<ActionFuture<BenchmarkResponse>> responses = new ArrayList<>();

            SearchRequest searchRequest = prepareBlockingScriptQuery();
            final int benches = between(1, 3);
            String[] names = new String[benches];
            for (int k = 0; k < benches; k++) {
                final BenchmarkRequest request =
                        BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap, searchRequest);
                request.settings().iterations(Integer.MAX_VALUE, true); // massive amount of iterations
                names[k] = BENCHMARK_NAME + Integer.toString(k);
                request.benchmarkName(names[k]);
                requests.add(request);
                logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                        request.settings().iterations());
            }

            boolean aborted = false;
            for (BenchmarkRequest r : requests) {
                final ActionFuture<BenchmarkResponse> benchmarkResponse = client().bench(r);
                responses.add(benchmarkResponse);
            }
            try {
                waitForQuery.await();
                if (benches > 1) {
                    awaitBusy(new Predicate<Object>() {
                        @Override
                        public boolean apply(java.lang.Object input) {
                            return client().prepareBenchStatus().get().benchmarkResponses().size() == benches;
                        }
                    });
                }
                final String badPatternA = "*z";
                final String badPatternB = "xxx";
                final String[] patterns;
                switch (getRandom().nextInt(3)) {
                    case 0:
                        patterns = new String [] {"*"};
                        break;
                    case 1:
                        patterns = new String[] {BENCHMARK_NAME_WILDCARD, badPatternA, badPatternB };
                        break;
                    case 2:
                        patterns = names;
                        break;
                    default:
                        patterns = new String [] {BENCHMARK_NAME_WILDCARD};
                }
                final AbortBenchmarkResponse abortResponse = client().prepareAbortBench(patterns).get();
                aborted = true;
                assertAcked(abortResponse);

                // Confirm that there are no active benchmarks in the cluster
                final BenchmarkStatusResponse statusResponse = client().prepareBenchStatus().execute().actionGet();
                waitForTestLatch.countDown(); // let the queries go - we already aborted and got the status
                assertThat(statusResponse.totalActiveBenchmarks(), equalTo(0));

                // Confirm that benchmark was indeed aborted
                for (ActionFuture<BenchmarkResponse> r : responses) {
                    assertThat(r.get().state(), is(BenchmarkResponse.State.ABORTED));
                }
            } finally {
                if (waitForTestLatch.getCount() == 1) {
                    waitForTestLatch.countDown();
                }
                if (!aborted) {
                    client().prepareAbortBench(BENCHMARK_NAME).get();
                }
                assertThat(waitForTestLatch.getCount(), is(0l));
            }
        }
    }

    @Test
    public void testAbortBenchmark() throws Exception {
        final int iters = between(1, 3); // we run this more than once to make sure metadata is cleaned up propperly
        for (int i = 0; i < iters ; i++) {
            SearchRequest searchRequest = prepareBlockingScriptQuery();
            final BenchmarkRequest request =
                    BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap, searchRequest);
            request.settings().iterations(Integer.MAX_VALUE, true); // massive amount of iterations
            logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                    request.settings().iterations());
            boolean aborted = false;
            final ActionFuture<BenchmarkResponse> benchmarkResponse = client().bench(request);
            try {
                waitForQuery.await();
                final AbortBenchmarkResponse abortResponse =
                        client().prepareAbortBench(BENCHMARK_NAME).get();
                aborted = true;
                // Confirm that the benchmark was actually aborted and did not finish on its own
                assertAcked(abortResponse);
                // Confirm that there are no active benchmarks in the cluster
                final BenchmarkStatusResponse statusResponse = client().prepareBenchStatus().execute().actionGet();
                waitForTestLatch.countDown(); // let the queries go - we already aborted and got the status
                assertThat(statusResponse.totalActiveBenchmarks(), equalTo(0));

                // Confirm that benchmark was indeed aborted
                assertThat(benchmarkResponse.get().state(), is(BenchmarkResponse.State.ABORTED));

            } finally {
                if (waitForTestLatch.getCount() == 1) {
                    waitForTestLatch.countDown();
                }
                if (!aborted) {
                    client().prepareAbortBench(BENCHMARK_NAME).get();
                }
                assertThat(waitForTestLatch.getCount(), is(0l));
            }
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
        double last = Double.NEGATIVE_INFINITY;
        for (Map.Entry<Double, Double> entry : percentiles.entrySet()) {
            assertThat(entry.getKey(), equalTo(BenchmarkSettings.DEFAULT_PERCENTILES[i++]));
            // This is a hedge against rounding errors. Sometimes two adjacent percentile values will
            // be nearly equivalent except for some insignificant decimal places. In such cases we
            // want the two values to compare as equal.
            assertThat(entry.getValue(), greaterThanOrEqualTo(last - 1e-6));
            last = entry.getValue();
        }
    }

    private String[] randomData() throws Exception {

        final int numIndices = scaledRandomIntBetween(1, 5);
        final String[] indices = new String[numIndices];

        for (int i = 0; i < numIndices; i++) {
            indices[i] = INDEX_PREFIX + i;
            final int numDocs = scaledRandomIntBetween(1, 100);
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

        flushAndRefresh();
        return indices;
    }
}
