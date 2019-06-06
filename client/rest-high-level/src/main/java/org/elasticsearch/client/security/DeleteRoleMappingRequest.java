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
import org.elasticsearch.common.Nullable;
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
