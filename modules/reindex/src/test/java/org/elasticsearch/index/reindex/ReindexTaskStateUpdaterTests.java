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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class ReindexTaskStateUpdaterTests extends ReindexTestCase {

    public void testEnsureLowerAssignmentFails() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId);

        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 1, ActionListener.wrap(() -> {}), () -> {});
        CountDownLatch successLatch = new CountDownLatch(1);

        updater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                successLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                successLatch.countDown();
                fail();
            }
        });
        successLatch.await();

        ReindexTaskStateUpdater oldAllocationUpdater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 0, ActionListener.wrap(() -> {}), () -> {});
        CountDownLatch failureLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        oldAllocationUpdater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                failureLatch.countDown();
                fail();
            }

            @Override
            public void onFailure(Exception exception) {
                exceptionRef.set(exception);
                failureLatch.countDown();
            }
        });
        failureLatch.await();
        assertThat(exceptionRef.get().getMessage(), equalTo("A newer task has already been allocated"));
    }

    public void testFailoverAssignmentFailsIfNonResilient() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId, false);

        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 0, ActionListener.wrap(() -> {}), () -> {});
        CountDownLatch successLatch = new CountDownLatch(1);

        updater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                successLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                successLatch.countDown();
                fail();
            }
        });
        successLatch.await();

        ReindexTaskStateUpdater failedOverUpdater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 1, ActionListener.wrap(() -> {}), () -> {});
        CountDownLatch failureLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        failedOverUpdater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                failureLatch.countDown();
                fail();
            }

            @Override
            public void onFailure(Exception exception) {
                exceptionRef.set(exception);
                failureLatch.countDown();
            }
        });
        failureLatch.await();
        assertThat(exceptionRef.get().getMessage(),
            equalTo("A prior task has already been allocated and reindexing is configured to be non-resilient"));
    }

    public void testEnsureHighestAllocationIsWinningAssignment() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId);
        CountDownLatch latch = new CountDownLatch(10);

        List<Integer> assignments = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        Collections.shuffle(assignments, random());

        for (Integer i : assignments) {
            ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(), taskId, i,
                ActionListener.wrap(() -> {}), () -> {});
            new Thread(() -> {
                updater.assign(new ActionListener<>() {
                    @Override
                    public void onResponse(ReindexTaskStateDoc stateDoc) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        assertThat(exception.getMessage(), containsString("A newer task has already been allocated"));
                        latch.countDown();
                    }
                });
            }).start();
        }

        latch.await();
        PlainActionFuture<ReindexTaskState> getFuture = PlainActionFuture.newFuture();
        reindexClient.getReindexTaskDoc(taskId, getFuture);
        ReindexTaskState reindexTaskState = getFuture.actionGet();
        assertEquals(9L, reindexTaskState.getStateDoc().getAllocationId().longValue());
    }

    public void testNewAllocationWillTriggerCancelCallback() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId);

        AtomicBoolean cancelled = new AtomicBoolean(false);
        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 0, ActionListener.wrap(() -> {}), () -> cancelled.set(true));
        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);

        updater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                firstAssignmentLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                firstAssignmentLatch.countDown();
                fail();
            }
        });
        firstAssignmentLatch.await();

        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);
        assertCheckpointBusy(taskId, reindexClient, 10);
        assertFalse(cancelled.get());

        ReindexTaskStateUpdater newAllocationUpdater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 1, ActionListener.wrap(() -> {}), () -> {});
        CountDownLatch secondAssignmentLatch = new CountDownLatch(1);

        newAllocationUpdater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                secondAssignmentLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                secondAssignmentLatch.countDown();
                fail();
            }
        });
        secondAssignmentLatch.await();

        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(20), status);
        assertBusy(() -> assertTrue(cancelled.get()));

        PlainActionFuture<ReindexTaskState> getFuture = PlainActionFuture.newFuture();
        reindexClient.getReindexTaskDoc(taskId, getFuture);
        ReindexTaskState reindexTaskState = getFuture.actionGet();
        assertEquals(10, reindexTaskState.getStateDoc().getCheckpoint().getRestartFromValue());
    }

    public void testFinishWillStopCheckpoints() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId);

        PlainActionFuture<ReindexTaskStateDoc> finishedFuture = PlainActionFuture.newFuture();
        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 0, finishedFuture, () -> {});
        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);

        updater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                firstAssignmentLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                firstAssignmentLatch.countDown();
                fail();
            }
        });
        firstAssignmentLatch.await();

        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);
        assertCheckpointBusy(taskId, reindexClient, 10);


        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.timeValueSeconds(5), status, Collections.emptyList(),
            Collections.emptyList(), false);
        updater.finish(response, null);
        finishedFuture.actionGet();

        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(20), status);

        PlainActionFuture<ReindexTaskState> getFuture = PlainActionFuture.newFuture();
        reindexClient.getReindexTaskDoc(taskId, getFuture);
        assertEquals(10, getFuture.actionGet().getStateDoc().getCheckpoint().getRestartFromValue());
    }

    public void testFinishStoresResult() throws Exception {
        String taskId = randomAlphaOfLength(10);
        ReindexIndexClient reindexClient = getReindexClient();
        createDoc(reindexClient, taskId);

        PlainActionFuture<ReindexTaskStateDoc> finishedFuture = PlainActionFuture.newFuture();
        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
            taskId, 0, finishedFuture, () -> {});
        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);

        updater.assign(new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskStateDoc stateDoc) {
                firstAssignmentLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                firstAssignmentLatch.countDown();
                fail();
            }
        });
        firstAssignmentLatch.await();

        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);

        assertCheckpointBusy(taskId, reindexClient, 10);

        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.timeValueSeconds(5), status, Collections.emptyList(),
            Collections.emptyList(), false);
        updater.finish(response, null);
        finishedFuture.actionGet();

        PlainActionFuture<ReindexTaskState> storedStateFuture = PlainActionFuture.newFuture();
        reindexClient.getReindexTaskDoc(taskId, storedStateFuture);
        ReindexTaskState storedState = storedStateFuture.actionGet();
        assertNotNull(storedState.getStateDoc().getReindexResponse());
        assertEquals(response.isTimedOut(), storedState.getStateDoc().getReindexResponse().isTimedOut());
        assertEquals(response.getSearchFailures(), storedState.getStateDoc().getReindexResponse().getSearchFailures());
        assertEquals(response.getBulkFailures(), storedState.getStateDoc().getReindexResponse().getBulkFailures());
        assertEquals(response.getTook(), storedState.getStateDoc().getReindexResponse().getTook());
        assertEquals(10, storedState.getStateDoc().getCheckpoint().getRestartFromValue());
    }

    private void createDoc(ReindexIndexClient client, String taskId) {
        createDoc(client, taskId, true);
    }

    private void createDoc(ReindexIndexClient client, String taskId, boolean resilient) {
        ReindexRequest request =
            reindex().source("source").destination("dest").refresh(true).request().setCheckpointInterval(TimeValue.ZERO);

        PlainActionFuture<ReindexTaskState> future = PlainActionFuture.newFuture();
        client.createReindexTaskDoc(taskId, new ReindexTaskStateDoc(request, resilient), future);
        future.actionGet();
    }

    private ReindexIndexClient getReindexClient() {
        ClusterService clusterService = clusterService();
        Client client = client(clusterService.getNodeName());
        return new ReindexIndexClient(client, clusterService, NamedXContentRegistry.EMPTY);
    }


    private void assertCheckpointBusy(String taskId, ReindexIndexClient reindexClient, int expected) throws Exception {
        assertBusy(() -> {
            PlainActionFuture<ReindexTaskState> future = PlainActionFuture.newFuture();
            reindexClient.getReindexTaskDoc(taskId, future);
            ReindexTaskState reindexTaskState = future.actionGet();
            assertNotNull(reindexTaskState.getStateDoc().getCheckpoint());
            assertEquals(expected, reindexTaskState.getStateDoc().getCheckpoint().getRestartFromValue());
        });
    }
}
