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
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class ReindexTaskStateUpdater implements Reindexer.CheckpointListener {

    private static final int MAX_ASSIGNMENT_ATTEMPTS = 10;

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    private final ReindexIndexClient reindexIndexClient;
    private final String persistentTaskId;
    private final long allocationId;
    private final Consumer<BulkByScrollTask.Status> committedCallback;
    private final Semaphore semaphore = new Semaphore(1);

    private int assignmentAttempts = 0;
    private ReindexTaskState lastState;
    private boolean isDone = false;

    public ReindexTaskStateUpdater(ReindexIndexClient reindexIndexClient, String persistentTaskId, long allocationId,
                                   Consumer<BulkByScrollTask.Status> committedCallback) {
        this.reindexIndexClient = reindexIndexClient;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
        // TODO: At some point I think we would like to replace a single universal callback to a listener that
        //  is passed to the checkpoint method and handles the version conflict
        this.committedCallback = committedCallback;
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
                            lastState = newTaskState;
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
        // TODO: Need some kind of throttling here, no need to do this all the time.
        // only do one checkpoint at a time, in case checkpointing is too slow.
        if (semaphore.tryAcquire()) {
            if (isDone) {
                semaphore.release();
            } else {
                ReindexTaskStateDoc nextState = lastState.getStateDoc().withCheckpoint(checkpoint, status);
                // TODO: This can fail due to conditional update. Need to hook into ability to cancel reindex process
                long term = lastState.getPrimaryTerm();
                long seqNo = lastState.getSeqNo();
                reindexIndexClient.updateReindexTaskDoc(persistentTaskId, nextState, term, seqNo, new ActionListener<>() {
                    @Override
                    public void onResponse(ReindexTaskState taskState) {
                        lastState = taskState;
                        committedCallback.accept(status);
                        semaphore.release();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        semaphore.release();
                    }
                });
            }
        }
    }

    public void finish(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                       ActionListener<ReindexTaskStateDoc> listener) {
        // TODO: Move to try acquire and a scheduled retry if there is currently contention
        semaphore.acquireUninterruptibly();
        if (isDone) {
            semaphore.release();
            listener.onFailure(new ElasticsearchException("Reindex task already finished locally"));
        } else {
            ReindexTaskStateDoc state = lastState.getStateDoc().withFinishedState(reindexResponse, exception);
            isDone = true;
            long term = lastState.getPrimaryTerm();
            long seqNo = lastState.getSeqNo();
            reindexIndexClient.updateReindexTaskDoc(persistentTaskId, state, term, seqNo, new ActionListener<>() {
                @Override
                public void onResponse(ReindexTaskState taskState) {
                    lastState = null;
                    semaphore.release();
                    listener.onResponse(taskState.getStateDoc());

                }

                @Override
                public void onFailure(Exception e) {
                    lastState = null;
                    semaphore.release();
                    listener.onFailure(e);
                }
            });
        }
    }
}
