/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * This class extends ActionRequestLazyBuilder with an implementation of the request() method. It is a convenience class to take care of
 * some of the boilerplate code.
 */
public abstract class ManagedActionRequestLazyBuilder<Request extends ActionRequest, Response extends ActionResponse> extends
    ActionRequestLazyBuilder<Request, Response> {

    protected ManagedActionRequestLazyBuilder(ElasticsearchClient client, ActionType<Response> action) {
        super(client, action);
    }

    public Request request() {
        validate();
        Request request = newEmptyInstance();
        apply(request);
        return request;
    }

    /**
     * This method is meant to be implemented by sub-classes. It should return a new instance of Request with minimal initialization
     * performed.
     * @return A clean new instance of the Request
     */
    protected abstract Request newEmptyInstance();

    /**
     * This method is meant to be implemented by sub-classes. It should throw an IllegalStateException with a useful error message if the
     * builder is unable to build its request for any reason.
     */
    protected void validate() throws IllegalStateException {}
}
