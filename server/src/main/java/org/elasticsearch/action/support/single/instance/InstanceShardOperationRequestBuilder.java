/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.action.ActionRequestLazyBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

public abstract class InstanceShardOperationRequestBuilder<
    Request extends InstanceShardOperationRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends InstanceShardOperationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestLazyBuilder<
        Request,
        Response> {
    private String index;
    private TimeValue timeout;

    protected InstanceShardOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action) {
        super(client, action);
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setIndex(String index) {
        this.index = index;
        return (RequestBuilder) this;
    }

    protected String getIndex() {
        return index;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (RequestBuilder) this;
    }

    protected void apply(Request request) {
        if (index != null) {
            request.index(index);
        }
        if (timeout != null) {
            request.timeout(timeout);
        }
    }
}
