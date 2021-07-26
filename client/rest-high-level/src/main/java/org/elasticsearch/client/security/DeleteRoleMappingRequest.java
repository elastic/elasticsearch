/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;

import java.util.Objects;

/**
 * Request object to delete a role mapping.
 */
public final class DeleteRoleMappingRequest implements Validatable {
    private final String name;
    private final RefreshPolicy refreshPolicy;

    /**
     * Constructor for DeleteRoleMappingRequest
     *
     * @param name role mapping name to be deleted
     * @param refreshPolicy refresh policy {@link RefreshPolicy} for the
     * request, defaults to {@link RefreshPolicy#getDefault()}
     */
    public DeleteRoleMappingRequest(final String name, @Nullable final RefreshPolicy refreshPolicy) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("role-mapping name is required");
        }
        this.name = name;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
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
        final DeleteRoleMappingRequest other = (DeleteRoleMappingRequest) obj;

        return (refreshPolicy == other.refreshPolicy) && Objects.equals(name, other.name);
    }

}
