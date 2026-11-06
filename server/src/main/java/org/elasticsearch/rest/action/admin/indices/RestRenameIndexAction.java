/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Renames an index in place. The route mirrors the resize APIs ({@code /{index}/_clone/{target}} and friends) because a rename is,
 * from the user's point of view, the same shape of operation: a source index and a target name. It is a shorthand for a
 * {@code POST _aliases} request with a single {@code rename_index} action, which is also how a rename is combined with alias
 * changes and index removals in one atomic cluster state update.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestRenameIndexAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "rename_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_rename/{target}"), new Route(PUT, "/{index}/_rename/{target}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var renameRequest = new IndicesAliasesRequest(getMasterNodeTimeout(request), getAckTimeout(request)).addAliasAction(
            IndicesAliasesRequest.AliasActions.renameIndex().index(request.param("index")).newIndex(request.param("target"))
        );
        return channel -> client.admin().indices().aliases(renameRequest, new RestToXContentListener<>(channel));
    }
}
