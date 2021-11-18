/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster.dangling;

import org.elasticsearch.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.ACCEPTED;

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

        importRequest.timeout(request.paramAsTime("timeout", importRequest.timeout()));
        importRequest.masterNodeTimeout(request.paramAsTime("master_timeout", importRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().importDanglingIndex(importRequest, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(AcknowledgedResponse acknowledgedResponse) {
                return ACCEPTED;
            }
        });
    }
}
