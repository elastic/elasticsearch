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

public class ReindexTaskStateUpdaterTests extends ReindexTestCase {

//    public void testEnsureLowerAssignmentFails() throws Exception {
//        String taskId = randomAlphaOfLength(10);
//        ReindexIndexClient reindexClient = getReindexClient();
//        createDoc(reindexClient, taskId);
//
//        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 1, (s) -> {});
//        CountDownLatch successLatch = new CountDownLatch(1);
//
//        updater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                successLatch.countDown();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                successLatch.countDown();
//                fail();
//            }
//        });
//        successLatch.await();
//
//        ReindexTaskStateUpdater oldAllocationUpdater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 0, (s) -> {});
//        CountDownLatch failureLatch = new CountDownLatch(1);
//        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
//
//        oldAllocationUpdater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                failureLatch.countDown();
//                fail();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                exceptionRef.set(exception);
//                failureLatch.countDown();
//            }
//        });
//        failureLatch.await();
//        assertThat(exceptionRef.get().getMessage(), equalTo("A newer task has already been allocated"));
//    }
//
//    public void testEnsureHighestAllocationIsWinningAssignment() throws Exception {
//        String taskId = randomAlphaOfLength(10);
//        ReindexIndexClient reindexClient = getReindexClient();
//        createDoc(reindexClient, taskId);
//        CountDownLatch latch = new CountDownLatch(10);
//
//        List<Integer> assignments = IntStream.range(0, 10).boxed().collect(Collectors.toList());
//        Collections.shuffle(assignments, random());
//
//        for (Integer i : assignments) {
//            ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(), taskId, i, (s) -> {});
//            new Thread(() -> {
//                updater.assign(new ActionListener<>() {
//                    @Override
//                    public void onResponse(ReindexTaskStateDoc stateDoc) {
//                        latch.countDown();
//                    }
//
//                    @Override
//                    public void onFailure(Exception exception) {
//                        assertThat(exception.getMessage(), containsString("A newer task has already been allocated"));
//                        latch.countDown();
//                    }
//                });
//            }).start();
//        }
//
//        latch.await();
//        PlainActionFuture<ReindexTaskState> future = PlainActionFuture.newFuture();
//        reindexClient.getReindexTaskDoc(taskId, future);
//        ReindexTaskState reindexTaskState = future.actionGet();
//        assertEquals(9L, reindexTaskState.getStateDoc().getAllocationId().longValue());
//    }
//
//    public void testNewAllocationWillStopCheckpoints() throws Exception {
//        String taskId = randomAlphaOfLength(10);
//        ReindexIndexClient reindexClient = getReindexClient();
//        createDoc(reindexClient, taskId);
//
//        AtomicInteger committed = new AtomicInteger(0);
//
//        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 0, (s) -> committed.incrementAndGet());
//        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);
//
//        updater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                firstAssignmentLatch.countDown();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                firstAssignmentLatch.countDown();
//                fail();
//            }
//        });
//        firstAssignmentLatch.await();
//
//        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
//        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);
//        assertBusy(() -> assertEquals(1, committed.get()));
//
//        ReindexTaskStateUpdater newAllocationUpdater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 1, (s) -> {});
//        CountDownLatch secondAssignmentLatch = new CountDownLatch(1);
//
//        newAllocationUpdater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                secondAssignmentLatch.countDown();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                secondAssignmentLatch.countDown();
//                fail();
//            }
//        });
//        secondAssignmentLatch.await();
//
//
//        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(20), status);
//        assertEquals(committed.get(), 1);
//
//        PlainActionFuture<ReindexTaskState> future = PlainActionFuture.newFuture();
//        reindexClient.getReindexTaskDoc(taskId, future);
//        ReindexTaskState reindexTaskState = future.actionGet();
//        assertEquals(10, reindexTaskState.getStateDoc().getCheckpoint().getRestartFromValue());
//    }
//
//    public void testFinishWillStopCheckpoints() throws Exception {
//        String taskId = randomAlphaOfLength(10);
//        ReindexIndexClient reindexClient = getReindexClient();
//        createDoc(reindexClient, taskId);
//
//        AtomicInteger committed = new AtomicInteger(0);
//
//        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 0, (s) -> committed.incrementAndGet());
//        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);
//
//        updater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                firstAssignmentLatch.countDown();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                firstAssignmentLatch.countDown();
//                fail();
//            }
//        });
//        firstAssignmentLatch.await();
//
//        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
//        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);
//        assertBusy(() -> assertEquals(1, committed.get()));
//
//
//        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.timeValueSeconds(5), status, Collections.emptyList(),
//            Collections.emptyList(), false);
//        PlainActionFuture<ReindexTaskStateDoc> finishedFuture = PlainActionFuture.newFuture();
//        updater.finish(response, null, finishedFuture);
//        finishedFuture.actionGet();
//
//        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(20), status);
//        assertEquals(committed.get(), 1);
//    }
//
//    public void testFinishStoresResult() throws Exception {
//        String taskId = randomAlphaOfLength(10);
//        ReindexIndexClient reindexClient = getReindexClient();
//        createDoc(reindexClient, taskId);
//
//        AtomicInteger committed = new AtomicInteger(0);
//
//        ReindexTaskStateUpdater updater = new ReindexTaskStateUpdater(reindexClient, client().threadPool(),
//            taskId, 0, (s) -> committed.incrementAndGet());
//        CountDownLatch firstAssignmentLatch = new CountDownLatch(1);
//
//        updater.assign(new ActionListener<>() {
//            @Override
//            public void onResponse(ReindexTaskStateDoc stateDoc) {
//                firstAssignmentLatch.countDown();
//            }
//
//            @Override
//            public void onFailure(Exception exception) {
//                firstAssignmentLatch.countDown();
//                fail();
//            }
//        });
//        firstAssignmentLatch.await();
//
//        BulkByScrollTask.Status status = new BulkByScrollTask.Status(Collections.emptyList(), null);
//        updater.onCheckpoint(new ScrollableHitSource.Checkpoint(10), status);
//        assertBusy(() -> assertEquals(1, committed.get()));
//
//
//        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.timeValueSeconds(5), status, Collections.emptyList(),
//            Collections.emptyList(), false);
//        PlainActionFuture<ReindexTaskStateDoc> finishedFuture = PlainActionFuture.newFuture();
//        updater.finish(response, null, finishedFuture);
//        finishedFuture.actionGet();
//
//        PlainActionFuture<ReindexTaskState> storedStateFuture = PlainActionFuture.newFuture();
//        reindexClient.getReindexTaskDoc(taskId, storedStateFuture);
//        ReindexTaskState storedState = storedStateFuture.actionGet();
//        assertNotNull(storedState.getStateDoc().getReindexResponse());
//        assertEquals(response.isTimedOut(), storedState.getStateDoc().getReindexResponse().isTimedOut());
//        assertEquals(response.getSearchFailures(), storedState.getStateDoc().getReindexResponse().getSearchFailures());
//        assertEquals(response.getBulkFailures(), storedState.getStateDoc().getReindexResponse().getBulkFailures());
//        assertEquals(response.getTook(), storedState.getStateDoc().getReindexResponse().getTook());
//        assertEquals(10, storedState.getStateDoc().getCheckpoint().getRestartFromValue());
//    }
//
//    private void createDoc(ReindexIndexClient client, String taskId) {
//        ReindexRequest request =
//            reindex().source("source").destination("dest").refresh(true).request().setCheckpointInterval(TimeValue.ZERO);
//
//        PlainActionFuture<ReindexTaskState> future = PlainActionFuture.newFuture();
//        client.createReindexTaskDoc(taskId, new ReindexTaskStateDoc(request), future);
//        future.actionGet();
//    }
//
//    private ReindexIndexClient getReindexClient() {
//        ClusterService clusterService = clusterService();
//        Client client = client(clusterService.getNodeName());
//        return new ReindexIndexClient(client, clusterService, NamedXContentRegistry.EMPTY);
//    }
}
