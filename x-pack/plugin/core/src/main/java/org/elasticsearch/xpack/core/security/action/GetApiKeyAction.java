/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Action for retrieving API key(s)
 */
public final class GetApiKeyAction extends Action<GetApiKeyRequest, GetApiKeyResponse, GetApiKeyRequestBuilder> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/get";
    public static final GetApiKeyAction INSTANCE = new GetApiKeyAction();

    private GetApiKeyAction() {
        super(NAME);
    }

    @Override
    public GetApiKeyResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<GetApiKeyResponse> getResponseReader() {
        return GetApiKeyResponse::new;
    }

    @Override
    public GetApiKeyRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetApiKeyRequestBuilder(client);
    }
}