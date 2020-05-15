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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.reindex.ReindexIndexClient.REINDEX_ALIAS;

public class ReindexTaskStateUpdater implements Reindexer.CheckpointListener {

    private static final long ONE_MINUTE_IN_MILLIS = TimeValue.timeValueMinutes(1).getMillis();
    private static final long THIRTY_MINUTES_IN_MILLIS = TimeValue.timeValueMinutes(30).millis();

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    private final ReindexIndexClient reindexIndexClient;
    private final ThreadPool threadPool;
    private final String persistentTaskId;
    private final long allocationId;
    private final TaskId ephemeralTaskId;
    private final ActionListener<ReindexTaskStateDoc> finishedListener;
    private final Runnable onCheckpointAssignmentConflict;
    private ThrottlingConsumer<Tuple<ScrollableHitSource.Checkpoint, BulkByScrollTask.Status>> checkpointThrottler;

    private ReindexTaskState lastState;
    private AtomicBoolean isDone = new AtomicBoolean();

    public ReindexTaskStateUpdater(ReindexIndexClient reindexIndexClient, ThreadPool threadPool, String persistentTaskId, long allocationId,
                                   TaskId ephemeralTaskId, ActionListener<ReindexTaskStateDoc> finishedListener,
                                   Runnable onCheckpointAssignmentConflict) {
        this.reindexIndexClient = reindexIndexClient;
        this.threadPool = threadPool;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
        this.ephemeralTaskId = ephemeralTaskId;
        this.finishedListener = finishedListener;
        this.onCheckpointAssignmentConflict = onCheckpointAssignmentConflict;
    }

    public void assign(ActionListener<ReindexTaskStateDoc> listener) {
        assign(listener, TimeValue.ZERO);
    }

    private void assign(ActionListener<ReindexTaskStateDoc> listener, TimeValue delay) {
        reindexIndexClient.getReindexTaskDoc(persistentTaskId, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                long term = taskState.getPrimaryTerm();
                long seqNo = taskState.getSeqNo();
                ReindexTaskStateDoc oldDoc = taskState.getStateDoc();

                assert oldDoc.getAllocationId() == null || allocationId != oldDoc.getAllocationId();

                ElasticsearchException assignmentFailureReason = assignmentFailureReason(oldDoc);

                if (assignmentFailureReason == null) {
                    ReindexTaskStateDoc newDoc = oldDoc.withNewAllocation(allocationId, ephemeralTaskId);
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
                            // TODO: Perhaps add external cancel functionality that will halt the updating process.
                            if (ex instanceof VersionConflictEngineException) {
                                // There has been an indexing operation since the GET operation. Try
                                // again if there are assignment attempts left.
                                logger.debug(new ParameterizedMessage("Failed to write to {} index on ASSIGNMENT due to version " +
                                    "conflict, retrying now [task-id={}]", REINDEX_ALIAS, persistentTaskId), ex);
                                assign(listener, delay);
                            } else {
                                TimeValue nextDelay = getNextDelay(delay);
                                logger.info(new ParameterizedMessage("Failed to write to {} index on ASSIGNMENT, retrying in {} " +
                                    "[task-id={}]", REINDEX_ALIAS, nextDelay, persistentTaskId), ex);
                                threadPool.schedule(() -> assign(listener, nextDelay), nextDelay, ThreadPool.Names.SAME);
                            }
                        }
                    });
                } else {
                    logger.info(new ParameterizedMessage("Failed to write ASSIGNMENT, will not retry"), assignmentFailureReason);
                    listener.onFailure(assignmentFailureReason);
                }
            }

            @Override
            public void onFailure(Exception ex) {
                TimeValue nextDelay = getNextDelay(delay);
                logger.info(new ParameterizedMessage("Failed to read from {} index on ASSIGNMENT, retrying in {} [task-id={}]",
                    REINDEX_ALIAS, nextDelay, persistentTaskId), ex);
                threadPool.schedule(() -> assign(listener, nextDelay), nextDelay, ThreadPool.Names.SAME);
            }
        });
    }

    private ElasticsearchException assignmentFailureReason(ReindexTaskStateDoc oldDoc) {
        if (oldDoc.isResilient()) {
            if (oldDoc.getAllocationId() == null || allocationId > oldDoc.getAllocationId()) {
                return null;
            } else {
                return new ElasticsearchException("A newer task has already been allocated");
            }
        } else {
            if (oldDoc.getAllocationId() == null) {
                return null;
            } else {
                return new ElasticsearchException("A prior task has already been allocated and reindexing is configured to be " +
                    "non-resilient");
            }

        }
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
                    logger.debug(new ParameterizedMessage("Failed to write to {} index on CHECKPOINT due to version conflict, " +
                        "verifying allocation now [task-id={}]", REINDEX_ALIAS, persistentTaskId), e);
                    reindexIndexClient.getReindexTaskDoc(persistentTaskId, new ActionListener<>() {
                        @Override
                        public void onResponse(ReindexTaskState reindexTaskState) {
                            ReindexTaskStateDoc doc = reindexTaskState.getStateDoc();
                            assert doc.getAllocationId() != null && doc.getAllocationId() >= allocationId;
                            if (allocationId != doc.getAllocationId()) {
                                // There has been a newer allocation, stop reindexing.
                                if (isDone.compareAndSet(false, true)) {
                                    logger.info("After allocation verification, allocation is not valid. Reindexing will be halted " +
                                        "[task-id={}]", persistentTaskId);
                                    onCheckpointAssignmentConflict.run();
                                }
                            } else {
                                lastState = reindexTaskState;
                                logger.info("After allocation verification, allocation still valid");
                            }
                            // Proceed regardless of whether the allocation is valid or not. If it is invalid,
                            // onCheckpointAssignmentConflict will stop the reindexing. If it is valid, we
                            // will try again on the next checkpoint.
                            whenDone.run();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // Unable to read from index. Just proceed and try again on the next checkpoint.
                            logger.info(new ParameterizedMessage("Failed to read from {} index on CHECKPOINT [task-id={}]",
                                REINDEX_ALIAS, persistentTaskId), e);
                            whenDone.run();
                        }
                    });
                } else {
                    logger.info(new ParameterizedMessage("Failed to write to {} index on CHECKPOINT [task-id={}]",
                            REINDEX_ALIAS, persistentTaskId),
                        e);
                    // Failed to write for other reason. Proceed and try again on the next checkpoint.
                    whenDone.run();
                }
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
        // Prevent backwards clock adjustments
        long endTimeMillis = Long.max(Instant.now().toEpochMilli(), lastState.getStateDoc().getStartTimeMillis());;
        ReindexTaskStateDoc state = lastState.getStateDoc().withFinishedState(endTimeMillis, reindexResponse, exception);
        long term = lastState.getPrimaryTerm();
        long seqNo = lastState.getSeqNo();

        reindexIndexClient.updateReindexTaskDoc(persistentTaskId, state, term, seqNo, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                lastState = taskState;
                finishedListener.onResponse(taskState.getStateDoc());
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof VersionConflictEngineException) {
                    logger.debug(new ParameterizedMessage("Failed to write to {} index on FINISHED due to version conflict, " +
                            "verifying allocation now [task-id={}]", REINDEX_ALIAS, persistentTaskId), e);
                    reindexIndexClient.getReindexTaskDoc(persistentTaskId, new ActionListener<>() {
                        @Override
                        public void onResponse(ReindexTaskState reindexTaskState) {
                            lastState = reindexTaskState;
                            ReindexTaskStateDoc doc = reindexTaskState.getStateDoc();
                            assert doc.getAllocationId() != null && doc.getAllocationId() >= allocationId;
                            // If allocation is still valid, try finished write again with no delay. If the
                            // allocation is not valid, do nothing. The process is already halted.
                            if (allocationId == doc.getAllocationId()) {
                                logger.debug("After allocation verification, allocation still valid. Retrying FINISHED now [task-id={}]",
                                    persistentTaskId);
                                writeFinishedState(reindexResponse, exception, delay);
                            } else {
                                logger.info("After allocation verification, allocation is not valid. Will not retry FINISHED [task-id={}]",
                                    persistentTaskId);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // Unable to read from index. Backoff and try again.
                            TimeValue nextDelay = getNextDelay(delay);
                            logger.info(new ParameterizedMessage("Failed to read from {} index on FINISHED, retrying in {}",
                                REINDEX_ALIAS, nextDelay), e);
                            reschedule(nextDelay, reindexResponse, exception);
                        }
                    });
                } else {
                    TimeValue nextDelay = getNextDelay(delay);
                    logger.info(new ParameterizedMessage("Failed to write to {} index on FINISHED, retrying in {} [task-id={}]",
                        REINDEX_ALIAS, nextDelay, persistentTaskId), e);
                    reschedule(nextDelay, reindexResponse, exception);
                }
            }

            private void reschedule(TimeValue nextDelay, @Nullable BulkByScrollResponse reindexResponse,
                                    @Nullable ElasticsearchException exception) {
                threadPool.scheduleUnlessShuttingDown(nextDelay, ThreadPool.Names.SAME,
                    () -> writeFinishedState(reindexResponse, exception, nextDelay));
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
