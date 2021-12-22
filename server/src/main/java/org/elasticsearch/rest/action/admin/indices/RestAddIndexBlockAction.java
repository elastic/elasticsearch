/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestAddIndexBlockAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}/_block/{block}"));
    }

    @Override
    public String getName() {
        return "add_index_block_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(
            IndexMetadata.APIBlock.fromName(request.param("block")),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        addIndexBlockRequest.masterNodeTimeout(request.paramAsTime("master_timeout", addIndexBlockRequest.masterNodeTimeout()));
        addIndexBlockRequest.timeout(request.paramAsTime("timeout", addIndexBlockRequest.timeout()));
        addIndexBlockRequest.indicesOptions(IndicesOptions.fromRequest(request, addIndexBlockRequest.indicesOptions()));
        return channel -> client.admin().indices().addBlock(addIndexBlockRequest, new RestToXContentListener<>(channel));
    }

}
