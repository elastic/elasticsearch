/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.privilege.SystemPrivilege;

import java.util.function.Predicate;

/**
 * Internal user that is applied to all requests made elasticsearch itself
 */
public class SystemUser extends User {

    public static final String NAME = UsernamesField.SYSTEM_NAME;
    public static final String ROLE_NAME = UsernamesField.SYSTEM_ROLE;

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

    public static boolean is(String principal) {
        return NAME.equals(principal);
    }

    public static boolean isAuthorized(String action) {
        return PREDICATE.test(action);
    }
}
