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

package org.elasticsearch.action.bulk.byscroll;

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
    private final AtomicArray<Tuple<BulkByScrollResponse, Exception>> results;
    private final AtomicInteger counter;

    public ParentBulkByScrollTask(long id, String type, String action, String description, TaskId parentTaskId, int slices) {
        super(id, type, action, description, parentTaskId);
        this.results = new AtomicArray<>(slices);
        this.counter  = new AtomicInteger(slices);
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
        for (AtomicArray.Entry<Tuple<BulkByScrollResponse, Exception>> t : results.asList()) {
            if (t.value != null) {
                if (t.value.v1() != null) {
                    sliceStatuses.set(t.index, new StatusOrException(t.value.v1().getStatus()));
                } else {
                    sliceStatuses.set(t.index, new StatusOrException(t.value.v2()));
                }
            }
        }
    }

    /**
     * Record a response from a slice and respond to the listener if the request is finished.
     */
    public void onSliceResponse(ActionListener<BulkByScrollResponse> listener, int sliceId, BulkByScrollResponse response) {
        results.setOnce(sliceId, new Tuple<>(response, null));
        /* If the request isn't finished we could automatically rethrottle the sub-requests here but we would only want to do that if we
         * were fairly sure they had a while left to go. */
        recordSliceCompletionAndRespondIfAllDone(listener);
    }

    /**
     * Record a failure from a slice and respond to the listener if the request is finished.
     */
    void onSliceFailure(ActionListener<BulkByScrollResponse> listener, int sliceId, Exception e) {
        results.setOnce(sliceId, new Tuple<>(null, e));
        recordSliceCompletionAndRespondIfAllDone(listener);
        // TODO cancel when a slice fails?
    }

    private void recordSliceCompletionAndRespondIfAllDone(ActionListener<BulkByScrollResponse> listener) {
        if (counter.decrementAndGet() != 0) {
            return;
        }
        List<BulkByScrollResponse> responses = new ArrayList<>(results.length());
        Exception exception = null;
        for (AtomicArray.Entry<Tuple<BulkByScrollResponse, Exception>> t : results.asList()) {
            if (t.value.v1() == null) {
                assert t.value.v2() != null : "exception shouldn't be null if value is null";
                if (exception == null) {
                    exception = t.value.v2();
                } else {
                    exception.addSuppressed(t.value.v2());
                }
            } else {
                assert t.value.v2() == null : "exception should be null if response is not null";
                responses.add(t.value.v1());
            }
        }
        if (exception == null) {
            listener.onResponse(new BulkByScrollResponse(responses, getReasonCancelled()));
        } else {
            listener.onFailure(exception);
        }
    }

}
