/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.fieldsenum.action;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class FieldsEnumRequestBuilder extends BroadcastOperationRequestBuilder<
    FieldsEnumRequest,
    FieldsEnumResponse,
    FieldsEnumRequestBuilder> {

    public FieldsEnumRequestBuilder(ElasticsearchClient client, FieldsEnumAction action) {
        super(client, action, new FieldsEnumRequest());
    }

    public FieldsEnumRequestBuilder setString(String string) {
        request.string(string);
        return this;
    }

    public FieldsEnumRequestBuilder setSize(int size) {
        request.size(size);
        return this;
    }

}
