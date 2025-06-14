/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestContentAggregator;
import org.elasticsearch.rest.RestRequest;

public class AggregatingDispatcher implements HttpServerTransport.Dispatcher {

    public void dispatchAggregatedRequest(RestRequest restRequest, RestChannel restChannel, ThreadContext threadContext) {}

    @Override
    public final void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        RestContentAggregator.aggregate(request, (r) -> dispatchAggregatedRequest(r, channel, threadContext));
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {

    }

}
