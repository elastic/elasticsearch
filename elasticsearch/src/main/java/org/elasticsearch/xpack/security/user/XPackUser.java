/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

/**
 * XPack internal user that manages xpack. Has all cluster/indices permissions for x-pack to operate.
 */
public class XPackUser extends User {

    public static final String NAME = "_xpack";
    private static final String ROLE_NAME = "superuser";
    public static final XPackUser INSTANCE = new XPackUser();

    private XPackUser() {
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
