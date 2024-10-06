/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequestLazyBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

public abstract class ReplicationRequestBuilder<
    Request extends ReplicationRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends ReplicationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestLazyBuilder<
        Request,
        Response> {
    private String index;
    private TimeValue timeout;
    private ActiveShardCount waitForActiveShards;

    protected ReplicationRequestBuilder(ElasticsearchClient client, ActionType<Response> action) {
        super(client, action);
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setIndex(String index) {
        this.index = index;
        return (RequestBuilder) this;
    }

    public String getIndex() {
        return index;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return (RequestBuilder) this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public RequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    protected void apply(Request request) {
        if (index != null) {
            request.index(index);
        }
        if (timeout != null) {
            request.timeout(timeout);
        }
        if (waitForActiveShards != null) {
            request.waitForActiveShards(waitForActiveShards);
        }
    }

}
