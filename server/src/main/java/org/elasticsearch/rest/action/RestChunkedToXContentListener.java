/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A REST based action listener that requires the response to implement {@link org.elasticsearch.common.xcontent.ChunkedToXContent}
 * and automatically builds an XContent based response.
 */
public final class RestChunkedToXContentListener<Response extends ChunkedToXContent> extends RestActionListener<Response> {

    public RestChunkedToXContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected void processResponse(Response response) throws IOException {
        channel.sendResponse(new RestResponse(RestStatus.OK, ChunkedRestResponseBody.fromXContent(response, channel.request(), channel)));
    }
}
