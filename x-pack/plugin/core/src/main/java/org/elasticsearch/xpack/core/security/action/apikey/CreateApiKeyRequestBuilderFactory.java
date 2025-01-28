/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.client.internal.Client;

public interface CreateApiKeyRequestBuilderFactory {
    CreateApiKeyRequestBuilder create(Client client);

    class Default implements CreateApiKeyRequestBuilderFactory {
        @Override
        public CreateApiKeyRequestBuilder create(Client client) {
            return new CreateApiKeyRequestBuilder(client);
        }
    }
}
