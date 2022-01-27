/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.diewithdignity;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestDieWithDignityAction extends BaseRestHandler {

    RestDieWithDignityAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_die_with_dignity"));
    }

    @Override
    public String getName() {
        return "die_with_dignity_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            /*
             * This is to force the size of the array to be non-deterministic so that a sufficiently smart compiler can not optimize away
             * getting the length of the array to a constant.
             */
            final int length = Randomness.get().nextBoolean() ? Integer.MAX_VALUE - 1 : Integer.MAX_VALUE;
            final long[] array = new long[length];
            // this is to force the array to be consumed so that it can not be optimized away by a sufficiently smart compiler
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                {
                    builder.field("length", array.length);
                }
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

}
