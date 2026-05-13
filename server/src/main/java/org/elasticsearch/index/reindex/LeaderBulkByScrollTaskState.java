/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
    private final SetOnce<Supplier<Optional<String>>> nodeToRelocateToSupplier;
    /**
     * The latest PIT ID from slice responses. Updated on each completion so we close the most recent context.
     */
    private final AtomicReference<BytesReference> latestPitId = new AtomicReference<>();

    /// The source-of-truth requests-per-second for this sliced task.
    /// Updated by rethrottle, read during relocation to patch per-slice RPS in ResumeInfo.
    /// Used to prevent race condition to ensure the customer doesn't get success on rethrottling, and then we relocate with old RPS.
    /// Guarded by {@code synchronized(this)} for rethrottle and relocation operations.
    private volatile float relocationRequestsPerSecond;
    private boolean capturedRpsForRelocation = false;

    public LeaderBulkByScrollTaskState(BulkByScrollTask task, int slices, float requestsPerSecond) {
        this.task = task;
        this.slices = slices;
        results = new AtomicArray<>(slices);
        runningSubtasks = new AtomicInteger(slices);
        this.nodeToRelocateToSupplier = new SetOnce<>();
        setRequestsPerSecondWithRelocationGuard(requestsPerSecond);
    }

    /**
     * Returns the number of slices this BulkByScrollRequest will use
     */
    public int getSlices() {
        return slices;
    }

    /**
     * Get the combined statuses of slice subtasks, merged with the given list of statuses.
     * Uses the leader's stored source-of-truth RPS rather than summing children, which can be stale after rethrottle
     * with completed slices.
     */
    public BulkByScrollTask.Status getStatus(List<BulkByScrollTask.StatusOrException> statuses) {
        // We only have access to the statuses of requests that have finished so we return them
        if (statuses.size() != results.length()) {
            throw new IllegalArgumentException("Given number of statuses does not match amount of expected results");
        }
        addResultsToList(statuses);
        return new BulkByScrollTask.Status(unmodifiableList(statuses), task.getReasonCancelled(), relocationRequestsPerSecond);
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
        if (response != null && response.getPitId().isPresent()) {
            latestPitId.set(response.getPitId().get());
        }
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

    public void setNodeToRelocateToSupplier(Supplier<Optional<String>> nodeToRelocateToSupplier) {
        this.nodeToRelocateToSupplier.set(Objects.requireNonNull(nodeToRelocateToSupplier));
    }

    public Optional<String> getNodeToRelocateTo() {
        final Supplier<Optional<String>> supplier = this.nodeToRelocateToSupplier.get();
        if (supplier == null) {
            throw new IllegalStateException("Node to relocate to supplier should be set before, if this method is called");
        }
        return supplier.get();
    }

    /// Updates the source-of-truth total RPS for this leader task. Called by rethrottle before fanning out to children.
    /// Throws 503 if the RPS has already been captured for relocation, meaning the task is mid-relocation and the
    /// caller should retry after the relocation completes. If we apply RPS then relocated task would resume with old RPS value.
    public synchronized void setRequestsPerSecondWithRelocationGuard(float rps) {
        if (rps <= 0) {
            throw new IllegalArgumentException("requests per second must be more than 0 but was [" + rps + "]");
        }
        if (capturedRpsForRelocation) {
            throw new ElasticsearchStatusException("cannot rethrottle, task is being relocated", RestStatus.SERVICE_UNAVAILABLE);
        }
        relocationRequestsPerSecond = rps;
    }

    /// Atomically reads the source-of-truth total RPS and sets a flag preventing further rethrottle. Called during relocation
    /// so that the captured value is consistent with what the destination will inherit.
    public synchronized float captureRequestsPerSecondForRelocation() {
        capturedRpsForRelocation = true;
        return relocationRequestsPerSecond;
    }

    private void recordSliceCompletionAndRespondIfAllDone(ActionListener<BulkByScrollResponse> listener) {
        if (runningSubtasks.decrementAndGet() != 0) {
            return;
        }

        if (task.isRelocationRequested() && getNodeToRelocateTo().isPresent()) {
            final BulkByScrollResponse relocationResponse = relocationResponseIfNeeded().orElse(null);
            if (relocationResponse != null) {
                listener.onResponse(relocationResponse);
                return;
            }
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
            listener.onResponse(
                new BulkByScrollResponse(responses, task.getReasonCancelled(), latestPitId.get(), relocationRequestsPerSecond)
            );
        } else {
            listener.onFailure(exception);
        }
    }

    private Optional<BulkByScrollResponse> relocationResponseIfNeeded() {
        final Map<Integer, ResumeInfo.SliceStatus> sliceResumeInfoMap = new HashMap<>();
        boolean allJobsCompletedThereforeNoNeedForRelocation = true;
        for (final Result result : results.asList()) {
            final var sliceStatus = getSliceStatus(result);
            if (sliceStatus.resumeInfo() != null) {
                allJobsCompletedThereforeNoNeedForRelocation = false;
            }
            sliceResumeInfoMap.put(result.sliceId, sliceStatus);
        }
        if (allJobsCompletedThereforeNoNeedForRelocation) {
            return Optional.empty();
        }
        final var resumeInfo = new ResumeInfo(task.relocationOrigin(), null, sliceResumeInfoMap);
        // this response is a local carrier for resumeInfo only — for higher-level code to handle relocation and then discard.
        // the status for the task that's serialized into the .tasks index is taken from the leader state.
        return Optional.of(
            new BulkByScrollResponse(
                TimeValue.MINUS_ONE,
                new BulkByScrollTask.Status(List.of(), null, 0f),
                List.of(),
                List.of(),
                false,
                resumeInfo
            )
        );
    }

    private static ResumeInfo.SliceStatus getSliceStatus(final Result result) {
        final var workerResumeInfo = Optional.ofNullable(result.response)
            .flatMap(BulkByScrollResponse::getTaskResumeInfo)
            .flatMap(resumeInfo -> {
                assert resumeInfo.worker() != null : "if taskResumeInfo present, worker should have resume info";
                assert resumeInfo.slices() == null : "if taskResumeInfo present, worker shouldn't have slices";
                return resumeInfo.getWorker();
            })
            .orElse(null);
        // even if we have slice failure(s), still relocate and run other slices to completion (current functionality without relocations)
        final var workerResult = workerResumeInfo == null ? new ResumeInfo.WorkerResult(result.response, result.failure) : null;
        return new ResumeInfo.SliceStatus(result.sliceId, workerResumeInfo, workerResult);
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
