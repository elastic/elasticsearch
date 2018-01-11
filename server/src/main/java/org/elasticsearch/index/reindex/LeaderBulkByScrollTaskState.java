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
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.tasks.TaskInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;

/**
 * Tracks the state of sliced subtasks and provides unified status information for a sliced BulkByScrollRequest.
 */
public class LeaderBulkByScrollTaskState {

    private final BulkByScrollTask task;

    private final int slices;
    /**
     * Holds the responses of slice workers as they come in
     */
    private final AtomicArray<Result> results;
    /**
     * How many subtasks are still running
     */
    private final AtomicInteger runningSubtasks;

    public LeaderBulkByScrollTaskState(BulkByScrollTask task, int slices) {
        this.task = task;
        this.slices = slices;
        results = new AtomicArray<>(slices);
        runningSubtasks = new AtomicInteger(slices);
    }

    /**
     * Returns the number of slices this BulkByScrollRequest will use
     */
    public int getSlices() {
        return slices;
    }

    /**
     * Get the combined statuses of slice subtasks, merged with the given list of statuses
     */
    public BulkByScrollTask.Status getStatus(List<BulkByScrollTask.StatusOrException> statuses) {
        // We only have access to the statuses of requests that have finished so we return them
        if (statuses.size() != results.length()) {
            throw new IllegalArgumentException("Given number of statuses does not match amount of expected results");
        }
        addResultsToList(statuses);
        return new BulkByScrollTask.Status(unmodifiableList(statuses), task.getReasonCancelled());
    }

    /**
     * Get the combined statuses of sliced subtasks
     */
    public BulkByScrollTask.Status getStatus() {
        return getStatus(Arrays.asList(new BulkByScrollTask.StatusOrException[results.length()]));
    }

    /**
     * The number of sliced subtasks that are still running
     */
    public int runningSliceSubTasks() {
        return runningSubtasks.get();
    }

    private void addResultsToList(List<BulkByScrollTask.StatusOrException> sliceStatuses) {
        for (Result t : results.asList()) {
            if (t.response != null) {
                sliceStatuses.set(t.sliceId, new BulkByScrollTask.StatusOrException(t.response.getStatus()));
            } else {
                sliceStatuses.set(t.sliceId, new BulkByScrollTask.StatusOrException(t.failure));
            }
        }
    }

    /**
     * Record a response from a slice and respond to the listener if the request is finished.
     */
    public void onSliceResponse(ActionListener<BulkByScrollResponse> listener, int sliceId, BulkByScrollResponse response) {
        results.setOnce(sliceId, new Result(sliceId, response));
        /* If the request isn't finished we could automatically rethrottle the sub-requests here but we would only want to do that if we
         * were fairly sure they had a while left to go. */
        recordSliceCompletionAndRespondIfAllDone(listener);
    }

    /**
     * Record a failure from a slice and respond to the listener if the request is finished.
     */
    public void onSliceFailure(ActionListener<BulkByScrollResponse> listener, int sliceId, Exception e) {
        results.setOnce(sliceId, new Result(sliceId, e));
        recordSliceCompletionAndRespondIfAllDone(listener);
        // TODO cancel when a slice fails?
    }

    private void recordSliceCompletionAndRespondIfAllDone(ActionListener<BulkByScrollResponse> listener) {
        if (runningSubtasks.decrementAndGet() != 0) {
            return;
        }
        List<BulkByScrollResponse> responses = new ArrayList<>(results.length());
        Exception exception = null;
        for (Result t : results.asList()) {
            if (t.response == null) {
                assert t.failure != null : "exception shouldn't be null if value is null";
                if (exception == null) {
                    exception = t.failure;
                } else {
                    exception.addSuppressed(t.failure);
                }
            } else {
                assert t.failure == null : "exception should be null if response is not null";
                responses.add(t.response);
            }
        }
        if (exception == null) {
            listener.onResponse(new BulkByScrollResponse(responses, task.getReasonCancelled()));
        } else {
            listener.onFailure(exception);
        }
    }

    private static final class Result {
        final BulkByScrollResponse response;
        final int sliceId;
        final Exception failure;

        private Result(int sliceId, BulkByScrollResponse response) {
            this.sliceId = sliceId;
            this.response = response;
            failure = null;
        }

        private Result(int sliceId, Exception failure) {
            this.sliceId = sliceId;
            this.failure = failure;
            response = null;
        }
    }
}
