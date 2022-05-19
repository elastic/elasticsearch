/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.LeaderBulkByScrollTaskState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helps parallelize reindex requests using sliced scrolls.
 */
class BulkByScrollParallelizationHelper {

    static final int AUTO_SLICE_CEILING = 20;

    private BulkByScrollParallelizationHelper() {}

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
        int configuredSlices = request.getSlices();
        if (configuredSlices == AbstractBulkByScrollRequest.AUTO_SLICES) {
            ClusterSearchShardsRequest shardsRequest = new ClusterSearchShardsRequest();
            shardsRequest.indices(request.getSearchRequest().indices());
            client.admin().cluster().searchShards(shardsRequest, listener.delegateFailure((l, response) -> {
                setWorkerCount(request, task, countSlicesBasedOnShards(response));
                l.onResponse(null);
            }));
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

        LeaderBulkByScrollTaskState worker = task.getLeaderState();
        int totalSlices = worker.getSlices();
        TaskId parentTaskId = new TaskId(localNodeId, task.getId());
        for (final SearchRequest slice : sliceIntoSubRequests(request.getSearchRequest(), IdFieldMapper.NAME, totalSlices)) {
            // TODO move the request to the correct node. maybe here or somehow do it as part of startup for reindex in general....
            Request requestForSlice = request.forSlice(parentTaskId, slice, totalSlices);
            ActionListener<BulkByScrollResponse> sliceListener = ActionListener.wrap(
                r -> worker.onSliceResponse(listener, slice.source().slice().getId(), r),
                e -> worker.onSliceFailure(listener, slice.source().slice().getId(), e)
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
