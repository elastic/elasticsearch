/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.SystemPrivilege;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Internal user that is applied to all requests made elasticsearch itself
 */
public class SystemUser extends InternalUser {

    public static final String NAME = UsernamesField.SYSTEM_NAME;

    @Deprecated
    public static final String ROLE_NAME = UsernamesField.SYSTEM_ROLE;

    /**
     * Package protected to enforce a singleton (private constructor) - use {@link InternalUsers#SYSTEM_USER} instead
     */
    static final SystemUser INSTANCE = new SystemUser();

    private static final Predicate<String> PREDICATE = SystemPrivilege.INSTANCE.predicate();

    private SystemUser() {
        super(NAME, Optional.empty(), Optional.empty());
    }

    /**
     * @return {@link Optional#empty()} because the {@code _system} user does not use role based security
     * @see #isAuthorized(String)
     */
    @Override
    public Optional<RoleDescriptor> getLocalClusterRoleDescriptor() {
        return Optional.empty();
    }

    @Deprecated
    public static boolean is(User user) {
        return InternalUsers.SYSTEM_USER.equals(user);
    }

    public static boolean isAuthorized(String action) {
        return PREDICATE.test(action);
    }
}
