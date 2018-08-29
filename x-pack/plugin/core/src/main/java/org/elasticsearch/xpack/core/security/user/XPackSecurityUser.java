/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

/**
 * internal user that manages xpack security. Has all cluster/indices permissions.
 */
public class XPackSecurityUser extends User {

    public static final String NAME = UsernamesField.XPACK_SECURITY_NAME;
    public static final XPackSecurityUser INSTANCE = new XPackSecurityUser();
    private static final String ROLE_NAME = UsernamesField.XPACK_SECURITY_ROLE;

    private XPackSecurityUser() {
        super(NAME, ROLE_NAME);
    }

    @Override
    public boolean equals(Object o) {
        return INSTANCE == o;
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
}
