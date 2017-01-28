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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

/**
 * Helps parallelize reindex requests using sliced scrolls.
 */
public class BulkByScrollParallelizationHelper {
    private BulkByScrollParallelizationHelper() {}

    public static <
                Request extends AbstractBulkByScrollRequest<Request>
            > void startSlices(Client client, TaskManager taskManager, Action<Request, BulkByScrollResponse, ?> action,
                    String localNodeId, ParentBulkByScrollTask task, Request request, ActionListener<BulkByScrollResponse> listener) {
        TaskId parentTaskId = new TaskId(localNodeId, task.getId());
        for (final SearchRequest slice : sliceIntoSubRequests(request.getSearchRequest(), UidFieldMapper.NAME, request.getSlices())) {
            // TODO move the request to the correct node. maybe here or somehow do it as part of startup for reindex in general....
            Request requestForSlice = request.forSlice(parentTaskId, slice);
            ActionListener<BulkByScrollResponse> sliceListener = ActionListener.wrap(
                    r -> task.onSliceResponse(listener, slice.source().slice().getId(), r),
                    e -> task.onSliceFailure(listener, slice.source().slice().getId(), e));
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
