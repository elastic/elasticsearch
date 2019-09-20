/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    /**
     * Constructor for creating request for enabling or disabling a built-in or native user
     * @param enabled if set to {@code true} will enable a user else if set to {@code false} to disable a user
     * @param username user name
     * @param refreshPolicy the refresh policy for the request. Defaults to {@link RefreshPolicy#IMMEDIATE}
     */
    SetUserEnabledRequest(boolean enabled, String username, RefreshPolicy refreshPolicy) {
        this.enabled = enabled;
        this.username = Objects.requireNonNull(username, "username is required");
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.IMMEDIATE : refreshPolicy;
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
