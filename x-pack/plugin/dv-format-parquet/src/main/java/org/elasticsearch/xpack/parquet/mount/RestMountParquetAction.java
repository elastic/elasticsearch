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

/** {@code POST _parquet/_mount}: mounts Parquet files as the backing indices of a data stream. */
public class RestMountParquetAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "parquet_mount_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_parquet/_mount"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ParquetMountRequest parquetRequest = ParquetMountRequest.fromXContent(request.contentParser());
        return channel -> client.execute(MountParquetAction.INSTANCE, parquetRequest, new RestToXContentListener<>(channel));
    }
}
