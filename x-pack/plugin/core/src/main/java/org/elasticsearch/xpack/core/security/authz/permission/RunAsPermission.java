/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;

import java.util.function.Predicate;

/**
 * A permissions that is based on a general privilege that contains patterns of users that this
 * user can execute a request as
 */
public final class RunAsPermission {

    public static final RunAsPermission NONE = new RunAsPermission(Privilege.NONE);

    private final Privilege privilege;
    private final Predicate<String> predicate;

    RunAsPermission(Privilege privilege) {
        this.privilege = privilege;
        this.predicate = privilege.predicate();
    }

    public Privilege getPrivilege() {
        return privilege;
    }

    /**
     * Checks if this permission grants run as to the specified user
     */
    public boolean check(String username) {
        return predicate.test(username);
    }
}
