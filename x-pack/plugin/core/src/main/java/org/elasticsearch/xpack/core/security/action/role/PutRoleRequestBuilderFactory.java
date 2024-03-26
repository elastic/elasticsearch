/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.client.internal.Client;

import java.util.function.Predicate;

public interface PutRoleRequestBuilderFactory {
    PutRoleRequestBuilder create(Client client, boolean restrictRequest, Predicate<String> fileRolesStoreNameChecker);

    class Default implements PutRoleRequestBuilderFactory {
        @Override
        public PutRoleRequestBuilder create(Client client, boolean restrictRequest, Predicate<String> fileRolesStoreNameChecker) {
            // by default, we don't apply extra restrictions to Put Role requests and don't require checks against file-based roles
            // these dependencies are only used by our stateless implementation
            return new PutRoleRequestBuilder(client);
        }
    }
}
