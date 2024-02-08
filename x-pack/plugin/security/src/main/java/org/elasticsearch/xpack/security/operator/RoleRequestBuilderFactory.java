/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;

public interface RoleRequestBuilderFactory {

    PutRoleRequestBuilder putRoleRequestBuilder(SecurityContext securityContext, Client client);

    class Default implements RoleRequestBuilderFactory {
        @Override
        public PutRoleRequestBuilder putRoleRequestBuilder(SecurityContext securityContext, Client client) {
            return new PutRoleRequestBuilder(client);
        }
    }

}
