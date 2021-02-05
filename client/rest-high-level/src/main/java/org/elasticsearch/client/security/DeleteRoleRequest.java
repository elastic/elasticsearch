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
 * A request delete a role from the security index
 */
public final class DeleteRoleRequest implements Validatable {

    private final String name;
    private final RefreshPolicy refreshPolicy;

    public DeleteRoleRequest(String name) {
        this(name,  RefreshPolicy.IMMEDIATE);
    }

    public DeleteRoleRequest(String name, RefreshPolicy refreshPolicy) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy is required");
    }

    public String getName() {
        return name;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}
