/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.function.Function;

/**
 * A REST based action listener that requires the response to implement {@link ToXContentObject} and automatically
 * builds an XContent based response.
 */
// TODO make this final
public class RestToXContentListener<Response extends ToXContentObject> extends RestBuilderListener<Response> {

    protected final Function<Response, RestStatus> statusFunction;
    private final Function<Response, String> locationFunction;

    public RestToXContentListener(RestChannel channel) {
        this(channel, r -> RestStatus.OK);
    }

    public RestToXContentListener(RestChannel channel, Function<Response, RestStatus> statusFunction) {
        this(channel, statusFunction, r -> {
            assert false : "Returned a 201 CREATED but not set up to support a Location header from " + r.getClass();
            return null;
        });
    }

    public RestToXContentListener(
        RestChannel channel,
        Function<Response, RestStatus> statusFunction,
        Function<Response, String> locationFunction
    ) {
        super(channel);
        this.statusFunction = statusFunction;
        this.locationFunction = locationFunction;
    }

    public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        assert response.isFragment() == false; // would be nice if we could make default methods final
        response.toXContent(builder, channel.request());
        RestStatus restStatus = statusFunction.apply(response);
        RestResponse r = new RestResponse(restStatus, builder);
        if (RestStatus.CREATED == restStatus) {
            final String location = locationFunction.apply(response);
            if (location != null) {
                r.addHeader("Location", location);
            }
        }
        return r;
    }
}
