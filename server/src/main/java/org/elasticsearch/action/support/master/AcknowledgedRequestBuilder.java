/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

/**
 * Base request builder for master node operations that support acknowledgements
 */
public abstract class AcknowledgedRequestBuilder<
    Request extends AcknowledgedRequest<Request>,
    Response extends AcknowledgedResponse,
    RequestBuilder extends AcknowledgedRequestBuilder<Request, Response, RequestBuilder>> extends MasterNodeOperationRequestBuilder<
        Request,
        Response,
        RequestBuilder> {

    protected AcknowledgedRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the maximum wait for acknowledgement from other nodes
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Timeout to wait for the operation to be acknowledged by current cluster nodes. Defaults
     * to {@code 10s}.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }
}
