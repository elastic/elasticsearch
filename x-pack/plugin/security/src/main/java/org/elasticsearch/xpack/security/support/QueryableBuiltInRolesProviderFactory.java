/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;

public interface QueryableBuiltInRolesProviderFactory {

    QueryableBuiltInRoles.Provider createProvider(ReservedRolesStore reservedRolesStore, FileRolesStore fileRolesStore);

    class Default implements QueryableBuiltInRolesProviderFactory {
        @Override
        public QueryableBuiltInRoles.Provider createProvider(ReservedRolesStore reservedRolesStore, FileRolesStore fileRolesStore) {
            return new QueryableReservedRolesProvider(reservedRolesStore);
        }
    }
}
