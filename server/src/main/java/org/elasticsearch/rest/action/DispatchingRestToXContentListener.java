/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.ExecutorService;

/**
 * Response listener for REST requests which dispatches the serialization of the response off of the thread on which the response was
 * received, since that thread is often a transport thread and XContent serialization might be expensive.
 */
public class DispatchingRestToXContentListener<Response extends ToXContentObject> extends RestActionListener<Response> {

    private final ExecutorService executor;
    private final RestRequest restRequest;

    public DispatchingRestToXContentListener(ExecutorService executor, RestChannel channel, RestRequest restRequest) {
        super(channel);
        this.executor = executor;
        this.restRequest = restRequest;
    }

    protected ToXContent.Params getParams() {
        return restRequest;
    }

    @Override
    protected void processResponse(Response response) {
        executor.execute(ActionRunnable.wrap(this, l -> new RestBuilderListener<Response>(channel) {
            @Override
            public RestResponse buildResponse(final Response response, final XContentBuilder builder) throws Exception {
                ensureOpen();
                response.toXContent(builder, getParams());
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        }.onResponse(response)));
    }

}
