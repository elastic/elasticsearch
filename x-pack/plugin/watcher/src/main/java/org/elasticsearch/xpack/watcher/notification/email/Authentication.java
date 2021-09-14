/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.xpack.core.watcher.common.secret.Secret;

import java.util.Objects;

public class Authentication {

    private final String user;
    private final Secret password;

    public Authentication(String user, Secret password) {
        this.user = user;
        this.password = password;
    }

    public String user() {
        return user;
    }

    public Secret password() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Authentication that = (Authentication) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, password);
    }
}
