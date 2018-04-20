/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;

import java.util.function.Predicate;

/**
 * A permission that is based on privileges for cluster wide actions
 */
public final class ClusterPermission {

    public static final ClusterPermission NONE = new ClusterPermission(ClusterPrivilege.NONE);

    private final ClusterPrivilege privilege;
    private final Predicate<String> predicate;

    ClusterPermission(ClusterPrivilege privilege) {
        this.privilege = privilege;
        this.predicate = privilege.predicate();
    }

    public ClusterPrivilege privilege() {
        return privilege;
    }

    public boolean check(String action) {
        return predicate.test(action);
    }
}
