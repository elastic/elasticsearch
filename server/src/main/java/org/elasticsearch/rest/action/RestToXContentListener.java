/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * A REST based action listener that assumes the response is of type {@link ToXContent} and automatically
 * builds an XContent based response (wrapping the toXContent in startObject/endObject).
 */
public class RestToXContentListener<Response extends ToXContentObject> extends RestResponseListener<Response> {

    public RestToXContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public final RestResponse buildResponse(Response response) throws Exception {
        return buildResponse(response, channel.newBuilder());
    }

    public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        assert response.isFragment() == false; //would be nice if we could make default methods final
        response.toXContent(builder, channel.request());
        return new BytesRestResponse(getStatus(response), builder);
    }

    protected RestStatus getStatus(Response response) {
        return RestStatus.OK;
    }
}
