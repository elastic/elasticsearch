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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;

/**
 * Task for parent bulk by scroll requests that have sub-workers.
 */
public class ParentBulkByScrollTask extends BulkByScrollTask {
    /**
     * Holds the responses as they come back. This uses {@link Tuple} as an "Either" style holder where only the response or the exception
     * is set.
     */
    private AtomicArray<Result> results;
    private AtomicInteger counter;

    // todo it may still make sense to store the original slices value in here even if it's not used
    // todo either enforce slices being set before calling task methods or separate that out into another class
    // todo probably want to look at the tests for this class and make sure the slices set behavior is enforced

    // todo this class may not be thread safe because the accesses to results and counter are not synchronized
    // e.g. it's possible to read all task results finished and runningSubtasks != 0 at the same time

    public ParentBulkByScrollTask(long id, String type, String action, String description, TaskId parentTaskId) {
        super(id, type, action, description, parentTaskId);
    }

    public void setSlices(int slices) {
        if (slices < 1) {
            throw new IllegalArgumentException("Slices must be at least one");
        }
        if (isSlicesSet()) {
            throw new IllegalStateException("Slices are already set");
        }
        results = new AtomicArray<>(slices);
        counter = new AtomicInteger(slices);
    }

    public boolean isSlicesSet() {
        return results != null && counter != null;
    }

    @Override
    public void rethrottle(float newRequestsPerSecond) {
        // Nothing to do because all rethrottling is done on slice sub tasks.
    }

    @Override
    public Status getStatus() {
        // We only have access to the statuses of requests that have finished so we return them
        List<StatusOrException> statuses = Arrays.asList(new StatusOrException[results.length()]);
        addResultsToList(statuses);
        return new Status(unmodifiableList(statuses), getReasonCancelled());
    }

    @Override
    public int runningSliceSubTasks() {
        return counter.get();
    }

    @Override
    public TaskInfo getInfoGivenSliceInfo(String localNodeId, List<TaskInfo> sliceInfo) {
        /* Merge the list of finished sub requests with the provided info. If a slice is both finished and in the list then we prefer the
         * finished status because we don't expect them to change after the task is finished. */
        List<StatusOrException> sliceStatuses = Arrays.asList(new StatusOrException[results.length()]);
        for (TaskInfo t : sliceInfo) {
            Status status = (Status) t.getStatus();
            sliceStatuses.set(status.getSliceId(), new StatusOrException(status));
        }
        addResultsToList(sliceStatuses);
        Status status = new Status(sliceStatuses, getReasonCancelled());
        return taskInfo(localNodeId, getDescription(), status);
    }

    private void addResultsToList(List<StatusOrException> sliceStatuses) {
        for (Result t : results.asList()) {
            if (t.response != null) {
                sliceStatuses.set(t.sliceId, new StatusOrException(t.response.getStatus()));
            } else {
                sliceStatuses.set(t.sliceId, new StatusOrException(t.failure));
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


    // todo this is not an atomic operation: you can read all the tasks results finished while counter is still not 0
    /*
     * e.g.
     * there are two child tasks
     *
     * thread A enters onSliceResponse
     * thread A sets a result for child task 1
     * thread B enters onSliceResponse
     * thread B enters a result for task 2
     * at this point all tasks are done
     * thread B decrements counter
     * thread A decrements counter and now thinks it's the last task even though it's not
     */
    /**
     * Record a failure from a slice and respond to the listener if the request is finished.
     */
    public void onSliceFailure(ActionListener<BulkByScrollResponse> listener, int sliceId, Exception e) {
        results.setOnce(sliceId, new Result(sliceId, e));
        recordSliceCompletionAndRespondIfAllDone(listener);
        // TODO cancel when a slice fails?
    }

    private void recordSliceCompletionAndRespondIfAllDone(ActionListener<BulkByScrollResponse> listener) {
        if (counter.decrementAndGet() != 0) {
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
            listener.onResponse(new BulkByScrollResponse(responses, getReasonCancelled()));
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
