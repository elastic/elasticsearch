/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.shield.authz.permission.SuperuserRole;
import org.elasticsearch.shield.user.User.ReservedUser;

/**
 * The reserved {@code elastic} superuser. As full permission/access to the cluster/indices and can
 * run as any other user.
 */
public class ElasticUser extends ReservedUser {

    public static final String NAME = "elastic";
    public static final String ROLE_NAME = SuperuserRole.NAME;
    public static final ElasticUser INSTANCE = new ElasticUser();

    private ElasticUser() {
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
