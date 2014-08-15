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

package org.elasticsearch.action.benchmark;

import org.apache.lucene.util.English;

import org.elasticsearch.action.benchmark.abort.BenchmarkAbortResponse;
import org.elasticsearch.action.benchmark.pause.BenchmarkPauseResponse;
import org.elasticsearch.action.benchmark.resume.BenchmarkResumeResponse;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusResponses;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.action.benchmark.BenchmarkTestUtil.INDEX_PREFIX;
import static org.elasticsearch.action.benchmark.BenchmarkTestUtil.INDEX_TYPE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Abstract base class for benchmark integration tests.
 */
public class AbstractBenchmarkTest extends ElasticsearchIntegrationTest {

    protected int numExecutorNodes = 0;
    protected String[] indices = Strings.EMPTY_ARRAY;
    protected Map<String, Map<String, BenchmarkSettings>> competitionSettingsMap;

    @Before
    public void baseBefore() throws Exception {
        numExecutorNodes = cluster().numBenchNodes();
    }

    protected Iterable<BenchmarkExecutorService> mockExecutorServices() {
        return internalCluster().getInstances(BenchmarkExecutorService.class);
    }

    protected MockBenchmarkCoordinatorService mockCoordinatorService() {

        // Don't use mock service class for getInstances(), otherwise we won't get the singleton.
        // Use the base service class instead and cast to the mock service
        final Iterable<BenchmarkCoordinatorService> services = internalCluster().getInstances(BenchmarkCoordinatorService.class);

        for (BenchmarkCoordinatorService service : services) {
            // The instance on the master node will have all the published meta-data changes
            if (((MockBenchmarkCoordinatorService) service).isOnMasterNode()) {
                return (MockBenchmarkCoordinatorService) service;
            }
        }
        fail("Unable to find mock benchmark coordinator service on master node");
        return null;
    }

    protected String[] randomData() throws Exception {

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

    protected void validateStatusRunning(final String benchmarkId) {

        final BenchmarkStatusResponses status = client().prepareBenchmarkStatus(benchmarkId).execute().actionGet();
        assertThat(status.responses().size(), equalTo(1));

        final BenchmarkStartResponse response = status.responses().get(0);
        assertThat(response.benchmarkId(), equalTo(benchmarkId));
        assertThat(response.state(), equalTo(BenchmarkStartResponse.State.RUNNING));
        assertFalse(response.hasErrors());
    }

    protected Tuple<CyclicBarrier, List<MockBenchmarkExecutorService.MockBenchmarkExecutor.FlowControl>> setUpFlowControl(
            final BenchmarkStartRequest request,
            final int competitorToPause,
            final int iterationToPauseBefore) throws InterruptedException {

        logger.info("--> Pausing competitor [{} (out of total competitors {})] before iteration [{} (out of total iterations {})]",
                request.competitors().get(competitorToPause).name(), request.competitors().size(), iterationToPauseBefore,
                request.competitors().get(competitorToPause).settings().iterations());

        final List<MockBenchmarkExecutorService.MockBenchmarkExecutor.FlowControl> controls = new ArrayList<>();
        final CyclicBarrier                           barrier  = new CyclicBarrier(request.numExecutorNodes() + 1);

        for (BenchmarkExecutorService mock : mockExecutorServices()) {

            final MockBenchmarkExecutorService.MockBenchmarkExecutor executor  = ((MockBenchmarkExecutorService) mock).executor();
            final Semaphore semaphore = new Semaphore(1);

            final MockBenchmarkExecutorService.MockBenchmarkExecutor.FlowControl control =
                    new MockBenchmarkExecutorService.MockBenchmarkExecutor.FlowControl(request.benchmarkId(), request.competitors().get(competitorToPause).name(),
                            iterationToPauseBefore,
                            semaphore, barrier);

            controls.add(control);
            semaphore.acquire();
            executor.addFlowControl(request.benchmarkId(), control);
        }

        return new Tuple<>(barrier, controls);
    }

    protected void validateStatusAborted(final String benchmarkId, final BenchmarkAbortResponse response) {

        validateBatchedResponseHasNodeState(benchmarkId, response, BenchmarkMetaData.Entry.NodeState.ABORTED);
    }

    protected void validateStatusPaused(final String benchmarkId, final BenchmarkPauseResponse response) {

        validateBatchedResponseHasNodeState(benchmarkId, response, BenchmarkMetaData.Entry.NodeState.PAUSED);
    }

    protected void validateStatusResumed(final String benchmarkId, final BenchmarkResumeResponse response) {

        validateBatchedResponseHasNodeState(benchmarkId, response, BenchmarkMetaData.Entry.NodeState.RUNNING);
    }

    protected void validateBatchedResponseHasNodeState(final String benchmarkId, final BatchedResponse response,
                                                     final BenchmarkMetaData.Entry.NodeState nodeState) {

        assertNotNull(response.getResponse(benchmarkId));

        final BatchedResponse.BenchmarkResponse br = response.getResponse(benchmarkId);
        final Map<String, BenchmarkMetaData.Entry.NodeState> nodeResponses = br.nodeResponses();
        assertThat(nodeResponses.size(), equalTo(numExecutorNodes));
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> entry : nodeResponses.entrySet()) {
            assertThat(entry.getValue(), equalTo(nodeState));
        }
    }
}
