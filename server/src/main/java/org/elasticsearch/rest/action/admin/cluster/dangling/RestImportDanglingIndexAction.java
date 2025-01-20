/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster.dangling;

import org.elasticsearch.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.ACCEPTED;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestImportDanglingIndexAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_dangling/{index_uuid}"));
    }

    @Override
    public String getName() {
        return "import_dangling_index";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final ImportDanglingIndexRequest importRequest = new ImportDanglingIndexRequest(
            request.param("index_uuid"),
            request.paramAsBoolean("accept_data_loss", false)
        );

        importRequest.ackTimeout(getAckTimeout(request));
        importRequest.masterNodeTimeout(getMasterNodeTimeout(request));

        return channel -> client.execute(
            TransportImportDanglingIndexAction.TYPE,
            importRequest,
            new RestToXContentListener<>(channel, r -> ACCEPTED)
        );
    }
}
