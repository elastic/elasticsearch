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

/**
 * Thin adapter that lets the ES|QL {@code EQL} source command delegate to the EQL search transport action.
 * <p>
 * The EQL plugin owns query planning and execution; this service simply forwards the (index, query) pair and
 * hands the raw {@link EqlSearchResponse} back to the caller. It runs on the coordinating node only, mirroring how
 * {@code InferenceService} wraps a {@link Client} to reach another plugin's transport action.
 */
public class EqlQueryService {

    private final Client client;
    private final ClusterService clusterService;

    public EqlQueryService(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Runs the EQL search and delivers the raw response.
     *
     * @param size       optional row limit pushed down from a following ES|QL {@code LIMIT}, forwarded as the EQL request
     *                   {@code size} (number of events / sequences); {@code null} leaves the EQL default in place.
     * @param parentTask the running ES|QL task; set as the EQL request's parent so cancelling the ES|QL query cancels the
     *                   EQL child action.
     */
    public void query(
        String index,
        String query,
        @Nullable Integer size,
        CancellableTask parentTask,
        ActionListener<EqlSearchResponse> listener
    ) {
        EqlSearchRequest request = new EqlSearchRequest();
        // The parser assembles a comma-joined pattern (FROM-style); split it into separate expressions here, the
        // same way RestEqlSearchAction does, so a multi-index pattern reaches the EQL endpoint as distinct indices.
        request.indices(Strings.splitStringByCommaToArray(index));
        request.query(query);
        if (size != null) {
            request.size(size);
        }
        if (parentTask != null) {
            request.setParentTask(new TaskId(clusterService.localNode().getId(), parentTask.getId()));
        }
        client.execute(EqlSearchAction.INSTANCE, request, listener);
    }
}
