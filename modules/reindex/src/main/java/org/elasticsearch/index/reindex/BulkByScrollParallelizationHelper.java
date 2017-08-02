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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helps parallelize reindex requests using sliced scrolls.
 */
class BulkByScrollParallelizationHelper {

    static final int AUTO_SLICE_CEILING = 20;

    private BulkByScrollParallelizationHelper() {}

    static <Request extends AbstractBulkByScrollRequest<Request>> void yourNameHere( //todo rename
        Request request,
        BulkByScrollTask task,
        Action<Request, BulkByScrollResponse, ?> action,
        ActionListener<BulkByScrollResponse> listener,
        Client client,
        DiscoveryNode node,
        Supplier<AbstractAsyncBulkByScrollAction<Request>> taskSupplier) {

        computeSlicing(client, request, listener, slices -> {
            if (slices > 1) {
                task.setParent(slices);
                sendSubRequests(client, action, node.getId(), task, request, listener);
            } else {
                Integer sliceId = request.getSearchRequest().source().slice() == null
                    ? null
                    : request.getSearchRequest().source().slice().getId();
                task.setChild(sliceId, request.getRequestsPerSecond());
                taskSupplier.get().start();
            }
        });
    }

    /**
     * Figures out how many slices to use when handling this request. If the {@code request} has slices set as a number, that number
     * will be used. If set to {@code "auto"}, it will compute it from the source indices' properties. The given consumer is passed the
     * resulting number of slices in either case.
     */
    private static <Request extends AbstractBulkByScrollRequest<Request>> void computeSlicing(
        Client client,
        Request request,
        ActionListener<BulkByScrollResponse> listener,
        Consumer<Integer> slicedBehavior) {

        Slices slices = request.getSlices();
        if (slices.isAuto()) {
            client.admin().cluster().prepareSearchShards(request.getSearchRequest().indices()).execute(ActionListener.wrap(
                response -> slicedBehavior.accept(countSlicesBasedOnShards(response)),
                listener::onFailure
            ));
        } else {
            slicedBehavior.accept(request.getSlices().number());
        }

    }

    private static int countSlicesBasedOnShards(ClusterSearchShardsResponse response) {
        Map<Index, Integer> countsByIndex = Arrays.stream(response.getGroups()).collect(Collectors.toMap(
            group -> group.getShardId().getIndex(),
            __ -> 1,
            (sum, term) -> sum + term
        ));
        Set<Integer> counts = new HashSet<>(countsByIndex.values());
        int leastShards = Collections.min(counts);
        return Math.min(leastShards, AUTO_SLICE_CEILING);
    }

    private static <Request extends AbstractBulkByScrollRequest<Request>> void sendSubRequests(
        Client client,
        Action<Request, BulkByScrollResponse, ?> action,
        String localNodeId,
        BulkByScrollTask task,
        Request request,
        ActionListener<BulkByScrollResponse> listener) {

        ParentBulkByScrollWorker worker = task.getParentWorker();
        int totalSlices = worker.getSlices();
        TaskId parentTaskId = new TaskId(localNodeId, task.getId());
        for (final SearchRequest slice : sliceIntoSubRequests(request.getSearchRequest(), UidFieldMapper.NAME, totalSlices)) {
            // TODO move the request to the correct node. maybe here or somehow do it as part of startup for reindex in general....
            Request requestForSlice = request.forSlice(parentTaskId, slice, totalSlices);
            ActionListener<BulkByScrollResponse> sliceListener = ActionListener.wrap(
                    r -> worker.onSliceResponse(listener, slice.source().slice().getId(), r),
                    e -> worker.onSliceFailure(listener, slice.source().slice().getId(), e));
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
                slicedSource = request.source().copyWithNewSlice(sliceBuilder);
            }
            slices[slice] = new SearchRequest()
                    .source(slicedSource)
                    .searchType(request.searchType())
                    .indices(request.indices())
                    .types(request.types())
                    .routing(request.routing())
                    .preference(request.preference())
                    .requestCache(request.requestCache())
                    .scroll(request.scroll())
                    .indicesOptions(request.indicesOptions());
        }
        return slices;
    }
}
