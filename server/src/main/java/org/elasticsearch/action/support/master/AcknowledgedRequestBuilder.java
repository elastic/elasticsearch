/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

/**
 * Base request builder for master node operations that support acknowledgements
 */
public abstract class AcknowledgedRequestBuilder<
    Request extends AcknowledgedRequest<Request>,
    Response extends ActionResponse & IsAcknowledgedSupplier,
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
        request.ackTimeout(timeout);
        return (RequestBuilder) this;
    }

}
