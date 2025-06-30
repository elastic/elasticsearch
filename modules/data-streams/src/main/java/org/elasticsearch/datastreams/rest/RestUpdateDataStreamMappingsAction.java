/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.UpdateDataStreamMappingsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateDataStreamMappingsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "update_data_stream_mappings_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}/_mappings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CompressedXContent mappings;
        try (XContentParser parser = request.contentParser()) {
            mappings = Template.parseMappings(parser);
        }
        boolean dryRun = request.paramAsBoolean("dry_run", false);

        UpdateDataStreamMappingsAction.Request updateDataStreamMappingsRequest = new UpdateDataStreamMappingsAction.Request(
            mappings,
            dryRun,
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request)
        ).indices(Strings.splitStringByCommaToArray(request.param("name")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            UpdateDataStreamMappingsAction.INSTANCE,
            updateDataStreamMappingsRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
