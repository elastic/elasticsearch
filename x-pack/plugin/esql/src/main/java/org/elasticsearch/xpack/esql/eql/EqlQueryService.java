/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.esql.plan.logical.eql.EqlQueryOptions;

/**
 * Thin coordinator-only adapter that forwards the ES|QL {@code EQL} source command to the EQL search transport action
 * and returns the raw {@link EqlSearchResponse}, mirroring how {@code InferenceService} wraps a {@link Client}.
 */
public class EqlQueryService {

    private final Client client;
    private final ClusterService clusterService;

    public EqlQueryService(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Runs the EQL search and delivers the raw response. {@code options} carries the {@code WITH}-map overrides
     * (tiebreaker / timestamp / event-category field; each {@code null} keeps the EQL default), {@code size} the
     * pushed-down {@code LIMIT} (null keeps EQL's default), and {@code parentTask} ties cancellation to the ES|QL task.
     */
    public void query(
        String index,
        String query,
        EqlQueryOptions options,
        @Nullable Integer size,
        CancellableTask parentTask,
        ActionListener<EqlSearchResponse> listener
    ) {
        EqlSearchRequest request = new EqlSearchRequest();
        // Split the FROM-style comma-joined pattern into distinct indices, as RestEqlSearchAction does.
        request.indices(Strings.splitStringByCommaToArray(index));
        request.query(query);
        if (options.tiebreakerField() != null) {
            request.tiebreakerField(options.tiebreakerField());
        }
        if (options.timestampField() != null) {
            request.timestampField(options.timestampField());
        }
        if (options.eventCategoryField() != null) {
            request.eventCategoryField(options.eventCategoryField());
        }
        if (size != null) {
            request.size(size);
        }
        if (parentTask != null) {
            request.setParentTask(new TaskId(clusterService.localNode().getId(), parentTask.getId()));
        }
        client.execute(EqlSearchAction.INSTANCE, request, listener);
    }
}
