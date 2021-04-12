/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termenum.action;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class TermEnumRequestBuilder extends BroadcastOperationRequestBuilder<TermEnumRequest, TermEnumResponse, TermEnumRequestBuilder> {

    public TermEnumRequestBuilder(ElasticsearchClient client, TermEnumAction action) {
        super(client, action, new TermEnumRequest());
    }

    public TermEnumRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    public TermEnumRequestBuilder setString(String string) {
        request.string(string);
        return this;
    }

    public TermEnumRequestBuilder setSize(int size) {
        request.size(size);
        return this;
    }

}
