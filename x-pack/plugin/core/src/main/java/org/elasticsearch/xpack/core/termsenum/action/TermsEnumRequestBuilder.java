/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class TermsEnumRequestBuilder extends BroadcastOperationRequestBuilder<
    TermsEnumRequest,
    TermsEnumResponse,
    TermsEnumRequestBuilder> {

    public TermsEnumRequestBuilder(ElasticsearchClient client, TermsEnumAction action) {
        super(client, action, new TermsEnumRequest());
    }

    public TermsEnumRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    public TermsEnumRequestBuilder setString(String string) {
        request.string(string);
        return this;
    }

    public TermsEnumRequestBuilder setSize(int size) {
        request.size(size);
        return this;
    }

}
