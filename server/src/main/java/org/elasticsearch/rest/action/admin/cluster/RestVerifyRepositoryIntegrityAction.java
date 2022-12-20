/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestVerifyRepositoryIntegrityAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_verify_integrity"));
    }

    @Override
    public String getName() {
        return "verify_repository_integrity_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var verifyRequest = new VerifyRepositoryIntegrityAction.Request(
            request.param("repository"),
            request.paramAsStringArray("indices", Strings.EMPTY_ARRAY),
            request.paramAsInt("thread_pool_concurrency", 0),
            request.paramAsInt("snapshot_verification_concurrency", 0),
            request.paramAsInt("index_verification_concurrency", 0),
            request.paramAsInt("index_snapshot_verification_concurrency", 0)
        );
        verifyRequest.masterNodeTimeout(request.paramAsTime("master_timeout", verifyRequest.masterNodeTimeout()));
        return channel -> {
            final var task = client.executeLocally(VerifyRepositoryIntegrityAction.INSTANCE, verifyRequest, ActionListener.noop());
            try (var builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", new TaskId(client.getLocalNodeId(), task.getId()).toString());
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }
}
