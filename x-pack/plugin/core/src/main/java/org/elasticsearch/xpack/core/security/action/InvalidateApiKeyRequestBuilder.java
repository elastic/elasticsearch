/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder for populating a {@link InvalidateApiKeyRequest}
 */
public final class InvalidateApiKeyRequestBuilder
        extends ActionRequestBuilder<InvalidateApiKeyRequest, InvalidateApiKeyResponse, InvalidateApiKeyRequestBuilder> {

    protected InvalidateApiKeyRequestBuilder(ElasticsearchClient client) {
        super(client, InvalidateApiKeyAction.INSTANCE, new InvalidateApiKeyRequest());
    }

}
