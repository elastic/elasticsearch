package org.elasticsearch.http;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public class NullDispatcher implements HttpServerTransport.Dispatcher {

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {

    }

    @Override
    public void dispatchBadRequest(RestRequest request, RestChannel channel, ThreadContext threadContext, Throwable cause) {

    }

}
