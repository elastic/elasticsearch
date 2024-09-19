/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.client.internal.Client;

public interface BulkPutRoleRequestBuilderFactory {
    BulkPutRoleRequestBuilder create(Client client);

    class Default implements BulkPutRoleRequestBuilderFactory {
        @Override
        public BulkPutRoleRequestBuilder create(Client client) {
            // This needs to be added when Bulk API is made public in serverless
            return new BulkPutRoleRequestBuilder(client);
        }
    }
}
