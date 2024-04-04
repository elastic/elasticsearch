/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.function.Predicate;

public interface ReservedRoleNameChecker {
    interface Factory {
        ReservedRoleNameChecker create(Predicate<String> fileRoleStoreNameChecker);

        class Default implements Factory {
            @Override
            public ReservedRoleNameChecker create(Predicate<String> fileRoleStoreNameChecker) {
                return new ReservedRoleNameChecker.Default();
            }
        }
    }

    boolean isReserved(String roleName);

    class Default implements ReservedRoleNameChecker {
        @Override
        public boolean isReserved(String roleName) {
            return ReservedRolesStore.isReserved(roleName);
        }
    }
}
