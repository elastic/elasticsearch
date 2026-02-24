/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.LeaderBulkByScrollTaskState;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES;

/**
 * Helps parallelize reindex requests using slices. This is search agnostic, working for both scrolls and PITs (point-in-times)
 */
class BulkByPaginatedSearchParallelizationHelper {

    static final int AUTO_SLICE_CEILING = 20;

    private BulkByPaginatedSearchParallelizationHelper() {}

    /**
     * Takes an action created by a {@link BulkByScrollTask} and runs it with regard to whether the request is sliced or not.
     *
     * If the request is not sliced (i.e. the number of slices is 1), the worker action in the given {@link Runnable} will be started on
     * the local node. If the request is sliced (i.e. the number of slices is more than 1), then a subrequest will be created for each
     * slice and sent.
     *
     * If slices are set as {@code "auto"}, this class will resolve that to a specific number based on characteristics of the source
     * indices. A request with {@code "auto"} slices may end up being sliced or unsliced.
     *
     * This method is equivalent to calling {@link #initTaskState} followed by {@link #executeSlicedAction}
     */
    static <Request extends AbstractBulkByScrollRequest<Request>> void startSlicedAction(
        Request request,
        BulkByScrollTask task,
        ActionType<BulkByScrollResponse> action,
        ActionListener<BulkByScrollResponse> listener,
        Client client,
        DiscoveryNode node,
        Runnable workerAction
    ) {
        initTaskState(
            task,
            request,
            client,
            listener.delegateFailure((l, v) -> executeSlicedAction(task, request, action, l, client, node, workerAction))
        );
    }

    /**
     * Takes an action and a {@link BulkByScrollTask} and runs it with regard to whether this task is a
     * leader or worker.
     *
     * If this task is a worker, the worker action in the given {@link Runnable} will be started on the local
     * node. If the task is a leader (i.e. the number of slices is more than 1), then a subrequest will be
     * created for each slice and sent.
     *
     * This method can only be called after the task state is initialized {@link #initTaskState}.
     */
    static <Request extends AbstractBulkByScrollRequest<Request>> void executeSlicedAction(
        BulkByScrollTask task,
        Request request,
        ActionType<BulkByScrollResponse> action,
        ActionListener<BulkByScrollResponse> listener,
        Client client,
        DiscoveryNode node,
        Runnable workerAction
    ) {
        if (task.isLeader()) {
            sendSubRequests(client, action, node.getId(), task, request, listener);
        } else if (task.isWorker()) {
            workerAction.run();
        } else {
            throw new AssertionError("Task should have been initialized at this point.");
        }
    }

    /**
     * Takes a {@link BulkByScrollTask} and ensures that its initial task state (leader or worker) is set.
     *
     * If slices are set as {@code "auto"}, this method will resolve that to a specific number based on
     * characteristics of the source indices. A request with {@code "auto"} slices may end up being sliced or
     * unsliced. This method does not execute the action. In order to execute the action see
     * {@link #executeSlicedAction}
     */
    static <Request extends AbstractBulkByScrollRequest<Request>> void initTaskState(
        BulkByScrollTask task,
        Request request,
        Client client,
        ActionListener<Void> listener
    ) {
        // If we are resuming a task, make sure we slice the same way as before. For example, if slicing was "auto", it's possible that the
        // number of shards in the source has changed since the original run, so we have to use the original number of slices.
        int configuredSlices = request.getResumeInfo().map(ResumeInfo::getTotalSlices).orElse(request.getSlices());
        assert request.getResumeInfo().isEmpty() || configuredSlices != AUTO_SLICES : "Resumed tasks can't have auto slices";
        if (configuredSlices == AUTO_SLICES) {
            client.execute(
                TransportClusterSearchShardsAction.TYPE,
                new ClusterSearchShardsRequest(request.getTimeout(), request.getSearchRequest().indices()),
                listener.safeMap(response -> {
                    setWorkerCount(request, task, countSlicesBasedOnShards(response));
                    return null;
                })
            );
        } else {
            setWorkerCount(request, task, configuredSlices);
            listener.onResponse(null);
        }
    }

    private static <Request extends AbstractBulkByScrollRequest<Request>> void setWorkerCount(
        Request request,
        BulkByScrollTask task,
        int slices
    ) {
        if (slices > 1) {
            task.setWorkerCount(slices);
        } else {
            SliceBuilder sliceBuilder = request.getSearchRequest().source().slice();
            Integer sliceId = sliceBuilder == null ? null : sliceBuilder.getId();
            task.setWorker(request.getRequestsPerSecond(), sliceId);
        }
    }

    private static int countSlicesBasedOnShards(ClusterSearchShardsResponse response) {
        Map<Index, Integer> countsByIndex = Arrays.stream(response.getGroups())
            .collect(Collectors.toMap(group -> group.getShardId().getIndex(), group -> 1, (sum, term) -> sum + term));
        Set<Integer> counts = new HashSet<>(countsByIndex.values());
        int leastShards = counts.isEmpty() ? 1 : Collections.min(counts);
        return Math.min(leastShards, AUTO_SLICE_CEILING);
    }

    private static <Request extends AbstractBulkByScrollRequest<Request>> void sendSubRequests(
        Client client,
        ActionType<BulkByScrollResponse> action,
        String localNodeId,
        BulkByScrollTask task,
        Request request,
        ActionListener<BulkByScrollResponse> listener
    ) {
        LeaderBulkByScrollTaskState leader = task.getLeaderState();
        int totalSlices = leader.getSlices();
        assert request.getResumeInfo().isEmpty() || totalSlices == request.getResumeInfo().get().getTotalSlices()
            : "If resuming, the total slices in the resume info should match the total slices in the task state";

        SearchRequest[] searchRequests = sliceIntoSubRequests(request.getSearchRequest(), IdFieldMapper.NAME, totalSlices);
        for (int sliceId = 0; sliceId < searchRequests.length; sliceId++) {
            // If a resumed slice was already completed, skip sending the request and directly record the result
            Optional<ResumeInfo> resumeInfo = request.getResumeInfo();
            if (resumeInfo.isPresent() && resumeInfo.get().isSliceCompleted(sliceId)) {
                WorkerResult sliceResult = resumeInfo.get().getSlice(sliceId).get().result();
                if (sliceResult.getResponse().isPresent()) {
                    leader.onSliceResponse(listener, sliceId, sliceResult.getResponse().get());
                } else if (sliceResult.getFailure().isPresent()) {
                    leader.onSliceFailure(listener, sliceId, sliceResult.getFailure().get());
                }
                continue;
            }

            TaskId parentTaskId = new TaskId(localNodeId, task.getId());
            SearchRequest searchRequest = searchRequests[sliceId];
            Request requestForSlice = request.forSlice(parentTaskId, searchRequest, totalSlices);
            ActionListener<BulkByScrollResponse> sliceListener = ActionListener.wrap(
                r -> leader.onSliceResponse(listener, searchRequest.source().slice().getId(), r),
                e -> leader.onSliceFailure(listener, searchRequest.source().slice().getId(), e)
            );
            client.execute(action, requestForSlice, sliceListener);
        }
    }

    /**
     * Slice a search request into {@code times} separate search requests slicing on {@code field}. Note that the slices are *shallow*
     * copies of this request so don't change them.
     */
    static SearchRequest[] sliceIntoSubRequests(SearchRequest request, String field, int times) {
        SearchRequest[] slices = new SearchRequest[times];
        for (int slice = 0; slice < times; slice++) {
            SliceBuilder sliceBuilder = new SliceBuilder(field, slice, times);
            SearchSourceBuilder slicedSource;
            if (request.source() == null) {
                slicedSource = new SearchSourceBuilder().slice(sliceBuilder);
            } else {
                if (request.source().slice() != null) {
                    throw new IllegalStateException("Can't slice a request that already has a slice configuration");
                }
                slicedSource = request.source().shallowCopy().slice(sliceBuilder);
            }
            SearchRequest searchRequest = new SearchRequest(request);
            searchRequest.source(slicedSource);
            slices[slice] = searchRequest;
        }
        return slices;
    }
}
