/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestDeleteAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in "
        + "document index requests is deprecated, use the /{index}/_doc/{id} endpoint instead.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(DELETE, "/{index}/_doc/{id}"),
                // Deprecated typed endpoint.
                new Route(DELETE, "/{index}/{type}/{id}")
            )
        );
    }

    @Override
    public String getName() {
        return "document_delete_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteRequest deleteRequest;
        if (request.hasParam("type")) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "delete_with_types", TYPES_DEPRECATION_MESSAGE);
            deleteRequest = new DeleteRequest(request.param("index"), request.param("type"), request.param("id"));
        } else {
            deleteRequest = new DeleteRequest(request.param("index"), request.param("id"));
        }

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

        return channel -> client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }
}
