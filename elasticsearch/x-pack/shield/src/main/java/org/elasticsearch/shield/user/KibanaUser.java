/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.shield.authz.permission.KibanaRole;
import org.elasticsearch.shield.user.User.ReservedUser;

/**
 *
 */
public class KibanaUser extends ReservedUser {

    public static final String NAME = "kibana";
    public static final String ROLE_NAME = KibanaRole.NAME;
    public static final KibanaUser INSTANCE = new KibanaUser();

    KibanaUser() {
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
}
