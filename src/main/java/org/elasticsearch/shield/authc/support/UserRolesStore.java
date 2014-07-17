/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

/**
 *
 */
public interface UserRolesStore {

    String[] roles(String username);

    public static interface Writable extends UserRolesStore {

        void setRoles(String username, String... roles);

        void addRoles(String username, String... roles);

        void removeRoles(String username, String... roles);

        void removeUser(String username);
    }
}
