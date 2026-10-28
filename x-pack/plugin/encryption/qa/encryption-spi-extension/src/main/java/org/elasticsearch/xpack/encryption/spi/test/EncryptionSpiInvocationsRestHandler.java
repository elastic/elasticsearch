/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption.spi.test;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Exposes the {@link TestEncryptedDataHandlerProvider#INVOCATIONS} counter via a REST endpoint so the javaRestTest can verify the
 * SPI handler is actually being invoked by the running rotation coordinator.
 */
class EncryptionSpiInvocationsRestHandler extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_test/encryption_spi/invocations"));
    }

    @Override
    public String getName() {
        return "test_encryption_spi_invocations";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        int count = TestEncryptedDataHandlerProvider.INVOCATIONS.get();
        return channel -> {
            try (XContentBuilder builder = JsonXContent.contentBuilder().startObject()) {
                builder.field("invocations", count);
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }
}
