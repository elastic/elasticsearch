/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.inference.action.StagedSemanticCleanupAction;
import org.elasticsearch.xpack.inference.action.StagedSemanticCleanupRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST /{index}/_semantic_cleanup}.
 *
 * <p>Clears expired staged semantic_text data from the target indices. Accepts an
 * optional {@code field} parameter to scope the cleanup to a single semantic_text
 * field, and an optional {@code max_age} parameter to override the cluster-level
 * staged TTL for this run.
 */
public class RestStagedSemanticCleanupAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "staged_semantic_cleanup_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_semantic_cleanup"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String field = request.param("field");
        TimeValue maxAge = request.paramAsTime("max_age", null);
        StagedSemanticCleanupRequest cleanupRequest = new StagedSemanticCleanupRequest(indices, field, maxAge);
        return channel -> client.execute(StagedSemanticCleanupAction.INSTANCE, cleanupRequest, new RestToXContentListener<>(channel));
    }
}
