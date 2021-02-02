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
 * Abstract request object to enable or disable a built-in or native user.
 */
public abstract class SetUserEnabledRequest implements Validatable {

    private final boolean enabled;
    private final String username;
    private final RefreshPolicy refreshPolicy;

    SetUserEnabledRequest(boolean enabled, String username, RefreshPolicy refreshPolicy) {
        this.enabled = enabled;
        this.username = Objects.requireNonNull(username, "username is required");
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getUsername() {
        return username;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}
