/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/{index}/_doc/{id}"));
    }

    @Override
    public String getName() {
        return "document_delete_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(request.param("index"), request.param("id"));
        deleteRequest.routing(request.param("routing"));
        deleteRequest.timeout(request.paramAsTime("timeout", DeleteRequest.DEFAULT_TIMEOUT));
        deleteRequest.setRefreshPolicy(request.param("refresh"));
        deleteRequest.version(RestActions.parseVersion(request));
        deleteRequest.versionType(VersionType.fromString(request.param("version_type"), deleteRequest.versionType()));
        deleteRequest.setIfSeqNo(request.paramAsLong("if_seq_no", deleteRequest.ifSeqNo()));
        deleteRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", deleteRequest.ifPrimaryTerm()));

        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            deleteRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }

        return channel -> client.delete(deleteRequest, new RestToXContentListener<>(channel, DeleteResponse::status));
    }
}
