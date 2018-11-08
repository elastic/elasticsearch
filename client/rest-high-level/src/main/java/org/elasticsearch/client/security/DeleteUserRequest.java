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
