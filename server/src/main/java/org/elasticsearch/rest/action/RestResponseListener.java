/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;

/**
 * A REST enabled action listener that has a basic onFailure implementation, and requires
 * sub classes to only implement {@link #buildResponse(Object)}.
 */
public abstract class RestResponseListener<Response> extends RestActionListener<Response> {

    protected RestResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected final void processResponse(Response response) throws Exception {
        channel.sendResponse(buildResponse(response));
    }

    /**
     * Builds the response to send back through the channel.
     */
    public abstract RestResponse buildResponse(Response response) throws Exception;
}
