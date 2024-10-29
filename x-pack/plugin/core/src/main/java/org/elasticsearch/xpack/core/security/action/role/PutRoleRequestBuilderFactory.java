/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.client.internal.Client;

public interface PutRoleRequestBuilderFactory {
    PutRoleRequestBuilder create(Client client);

    class Default implements PutRoleRequestBuilderFactory {
        @Override
        public PutRoleRequestBuilder create(Client client) {
            return new PutRoleRequestBuilder(client);
        }
    }
}
