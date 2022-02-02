/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public class NullDispatcher implements HttpServerTransport.Dispatcher {

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {

    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {

    }

}
