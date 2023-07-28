/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A multi get document action request builder.
 */
public class MultiGetRequestBuilder extends ActionRequestBuilder<MultiGetRequest, MultiGetResponse> {

    public MultiGetRequestBuilder(ElasticsearchClient client, MultiGetAction action) {
        super(client, action, new MultiGetRequest());
    }

    public MultiGetRequestBuilder add(String index, String id) {
        request.add(index, id);
        return this;
    }

    public MultiGetRequestBuilder addIds(String index, Iterable<String> ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiGetRequestBuilder addIds(String index, String... ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiGetRequestBuilder add(MultiGetRequest.Item item) {
        request.add(item);
        return this;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to {@code true}. Defaults
     * to {@code false}.
     */
    public MultiGetRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public MultiGetRequestBuilder setRealtime(boolean realtime) {
        request.realtime(realtime);
        return this;
    }
}
