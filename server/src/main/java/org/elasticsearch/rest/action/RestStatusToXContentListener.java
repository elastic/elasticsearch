/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action;

import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.function.Function;

/**
 * Content listener that extracts that {@link RestStatus} from the response.
 */
public class RestStatusToXContentListener<Response extends StatusToXContentObject> extends RestToXContentListener<Response> {
    private final Function<Response, String> extractLocation;

    /**
     * Build an instance that doesn't support responses with the status {@code 201 CREATED}.
     */
    public RestStatusToXContentListener(RestChannel channel) {
        this(channel, r -> {
            assert false: "Returned a 201 CREATED but not set up to support a Location header";
            return null;
        });
    }

    /**
     * Build an instance that does support responses with the status {@code 201 CREATED}.
     */
    public RestStatusToXContentListener(RestChannel channel, Function<Response, String> extractLocation) {
        super(channel);
        this.extractLocation = extractLocation;
    }

    @Override
    public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        assert response.isFragment() == false; //would be nice if we could make default methods final
        response.toXContent(builder, channel.request());
        RestResponse restResponse = new BytesRestResponse(response.status(), builder);
        if (RestStatus.CREATED == restResponse.status()) {
            final String location = extractLocation.apply(response);
            if (location != null) {
                restResponse.addHeader("Location", location);
            }
        }
        return restResponse;
    }
}
