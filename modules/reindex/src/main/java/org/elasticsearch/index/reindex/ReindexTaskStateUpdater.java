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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReindexTaskStateUpdater implements Reindexer.CheckpointListener {

    private static final int MAX_ASSIGNMENT_ATTEMPTS = 10;
    private static final long ONE_MINUTE_IN_MILLIS = TimeValue.timeValueMinutes(1).getMillis();
    private static final long THIRTY_MINUTES_IN_MILLIS = TimeValue.timeValueMinutes(30).millis();

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    private final ReindexIndexClient reindexIndexClient;
    private final ThreadPool threadPool;
    private final String persistentTaskId;
    private final long allocationId;
    private final ActionListener<ReindexTaskStateDoc> finishedListener;
    private final Runnable onCheckpointAssignmentConflict;
    private ThrottlingConsumer<Tuple<ScrollableHitSource.Checkpoint, BulkByScrollTask.Status>> checkpointThrottler;

    private int assignmentAttempts = 0;
    private ReindexTaskState lastState;
    private AtomicBoolean isDone = new AtomicBoolean();

    public ReindexTaskStateUpdater(ReindexIndexClient reindexIndexClient, ThreadPool threadPool, String persistentTaskId, long allocationId,
                                   ActionListener<ReindexTaskStateDoc> finishedListener, Runnable onCheckpointAssignmentConflict) {
        this.reindexIndexClient = reindexIndexClient;
        this.threadPool = threadPool;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
        this.finishedListener = finishedListener;
        this.onCheckpointAssignmentConflict = onCheckpointAssignmentConflict;
    }

    public void assign(ActionListener<ReindexTaskStateDoc> listener) {
        ++assignmentAttempts;
        reindexIndexClient.getReindexTaskDoc(persistentTaskId, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                long term = taskState.getPrimaryTerm();
                long seqNo = taskState.getSeqNo();
                ReindexTaskStateDoc oldDoc = taskState.getStateDoc();

                assert oldDoc.getAllocationId() == null || allocationId != oldDoc.getAllocationId();
                if (oldDoc.getAllocationId() == null || allocationId > oldDoc.getAllocationId()) {
                    ReindexTaskStateDoc newDoc = oldDoc.withNewAllocation(allocationId);
                    reindexIndexClient.updateReindexTaskDoc(persistentTaskId, newDoc, term, seqNo, new ActionListener<>() {
                        @Override
                        public void onResponse(ReindexTaskState newTaskState) {
                            assert checkpointThrottler == null;
                            lastState = newTaskState;
                            checkpointThrottler = new ThrottlingConsumer<>(
                                (t, whenDone) -> updateCheckpoint(t.v1(), t.v2(), whenDone),
                                newTaskState.getStateDoc().getReindexRequest().getCheckpointInterval(), System::nanoTime, threadPool
                            );
                            listener.onResponse(newTaskState.getStateDoc());
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            if (ex instanceof VersionConflictEngineException) {
                                // There has been an indexing operation since the GET operation. Try
                                // again if there are assignment attempts left.
                                // TODO: Perhaps add external cancel functionality that will halts the updating process.
                                if (assignmentAttempts < MAX_ASSIGNMENT_ATTEMPTS) {
                                    int nextAttempt = assignmentAttempts + 1;
                                    logger.debug("Attempting to retry reindex task assignment write. Attempt number " + nextAttempt);
                                    assign(listener);
                                } else {
                                    String message = "Failed to write allocation id to reindex task doc after " + MAX_ASSIGNMENT_ATTEMPTS
                                        + "retry attempts";
                                    logger.info(message, ex);
                                    listener.onFailure(new ElasticsearchException(message, ex));
                                }
                            } else {
                                String message = "Failed to write allocation id to reindex task doc";
                                logger.info(message, ex);
                                listener.onFailure(new ElasticsearchException(message, ex));
                            }
                        }
                    });
                } else {
                    ElasticsearchException ex = new ElasticsearchException("A newer task has already been allocated");
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception ex) {
                String message = "Failed to fetch reindex task doc";
                logger.info(message, ex);
                listener.onFailure(new ElasticsearchException(message, ex));
            }
        });
    }

    @Override
    public void onCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status) {
        assert checkpointThrottler != null;

        checkpointThrottler.accept(Tuple.tuple(checkpoint, status));
    }

    private void updateCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status, Runnable whenDone) {
        ReindexTaskStateDoc nextState = lastState.getStateDoc().withCheckpoint(checkpoint, status);
        long term = lastState.getPrimaryTerm();
        long seqNo = lastState.getSeqNo();
        reindexIndexClient.updateReindexTaskDoc(persistentTaskId, nextState, term, seqNo, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                lastState = taskState;
                whenDone.run();
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof VersionConflictEngineException) {
                    // TODO: Need to ensure that the allocation has changed
                    if (isDone.compareAndSet(false, true)) {
                        onCheckpointAssignmentConflict.run();
                    }
                }
                whenDone.run();
            }
        });
    }

    public void finish(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception) {
        assert checkpointThrottler != null;
        if (isDone.compareAndSet(false, true)) {
            checkpointThrottler.close(() -> writeFinishedState(reindexResponse, exception, TimeValue.ZERO));
        }
    }

    private void writeFinishedState(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                                    TimeValue delay) {
        ReindexTaskStateDoc state = lastState.getStateDoc().withFinishedState(reindexResponse, exception);
        long term = lastState.getPrimaryTerm();
        long seqNo = lastState.getSeqNo();

        reindexIndexClient.updateReindexTaskDoc(persistentTaskId, state, term, seqNo, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                finishedListener.onResponse(taskState.getStateDoc());

            }

            @Override
            public void onFailure(Exception e) {
                // TODO: Need to ensure that the allocation has changed
                if (e instanceof VersionConflictEngineException == false) {
                    TimeValue nextDelay = getNextDelay(delay);
                    threadPool.schedule(() -> writeFinishedState(reindexResponse, exception, nextDelay), nextDelay, ThreadPool.Names.SAME);
                }
            }
        });
    }

    private TimeValue getNextDelay(TimeValue delay) {
        TimeValue newDelay;
        if (TimeValue.ZERO.equals(delay)) {
            newDelay = TimeValue.timeValueMillis(500);
        } else if (delay.getMillis() < ONE_MINUTE_IN_MILLIS) {
            newDelay = TimeValue.timeValueMillis(delay.getMillis() * 2);
        } else {
            newDelay = TimeValue.timeValueMillis(Math.max(delay.getMillis() + ONE_MINUTE_IN_MILLIS, THIRTY_MINUTES_IN_MILLIS));
        }
        return newDelay;
    }
}
