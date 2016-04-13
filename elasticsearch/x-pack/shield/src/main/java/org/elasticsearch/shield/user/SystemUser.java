/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.shield.authz.privilege.SystemPrivilege;

import java.util.function.Predicate;

/**
 * Shield internal user that manages the {@code .shield}
 * index. Has permission to monitor the cluster as well as all actions that deal
 * with the shield admin index.
 */
public class SystemUser extends User {

    public static final String NAME = "__es_system_user";
    public static final String ROLE_NAME = "__es_system_role";

    public static final User INSTANCE = new SystemUser();

    private static final Predicate<String> PREDICATE = SystemPrivilege.INSTANCE.predicate();

    private SystemUser() {
        super(NAME, ROLE_NAME);
    }

    @Override
    public boolean equals(Object o) {
        return o == INSTANCE;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }

    public static boolean isAuthorized(String action) {
        return PREDICATE.test(action);
    }
}
