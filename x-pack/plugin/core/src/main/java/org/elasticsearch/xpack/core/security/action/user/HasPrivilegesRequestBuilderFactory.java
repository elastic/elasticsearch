/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.client.internal.Client;

public interface HasPrivilegesRequestBuilderFactory {
    HasPrivilegesRequestBuilder create(Client client, boolean restrictRequest);

    class Default implements HasPrivilegesRequestBuilderFactory {

        @Override
        public HasPrivilegesRequestBuilder create(Client client, boolean restrictRequest) {
            assert false == restrictRequest;
            return new HasPrivilegesRequestBuilder(client);
        }
    }
}
