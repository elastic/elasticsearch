/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local.model;

public class User {
    public static final String ROOT_USER_ROLE = "_es_test_root";
    public static final User DEFAULT_USER = new User("test_user", "x-pack-test-password", ROOT_USER_ROLE, true);

    private final String username;
    private final String password;
    private final String role;
    private final boolean operator;

    public User(String username, String password) {
        this.username = username;
        this.password = password;
        this.role = ROOT_USER_ROLE;
        this.operator = true;
    }

    public User(String username, String password, String role, boolean operator) {
        this.username = username;
        this.password = password;
        this.role = role;
        this.operator = operator;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }

    public boolean isOperator() {
        return operator;
    }
}
