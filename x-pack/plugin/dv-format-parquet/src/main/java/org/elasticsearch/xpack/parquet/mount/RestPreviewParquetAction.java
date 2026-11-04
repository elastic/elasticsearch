/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/** {@code POST _parquet/_preview}: what a mount of the files would create. */
public class RestPreviewParquetAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "parquet_preview_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_parquet/_preview"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ParquetMountRequest parquetRequest = ParquetMountRequest.fromXContent(request.contentParser());
        return channel -> client.execute(PreviewParquetAction.INSTANCE, parquetRequest, new RestToXContentListener<>(channel));
    }
}
