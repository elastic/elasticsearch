/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * A request to delete a user from the native realm.
 */
public final class DeleteUserRequest implements Validatable {

    private final String name;
    private final RefreshPolicy refreshPolicy;

    public DeleteUserRequest(String name) {
        this(name,  RefreshPolicy.IMMEDIATE);
    }

    public DeleteUserRequest(String name, RefreshPolicy refreshPolicy) {
        this.name = Objects.requireNonNull(name, "user name is required");
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy is required");
    }

    public String getName() {
        return name;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, refreshPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DeleteUserRequest other = (DeleteUserRequest) obj;

        return (refreshPolicy == other.refreshPolicy) && Objects.equals(name, other.name);
    }
}
