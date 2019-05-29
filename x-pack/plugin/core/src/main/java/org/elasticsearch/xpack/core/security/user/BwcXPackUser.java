/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.user;

/**
 * The representation of the XPackUser prior to version 5.6.1. This class should only be used for BWC purposes!
 */
@Deprecated
public final class BwcXPackUser extends User {

    public static final String NAME = XPackUser.NAME;
    public static final String ROLE_NAME = "superuser";
    public static final BwcXPackUser INSTANCE = new BwcXPackUser();

    private BwcXPackUser() {
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
