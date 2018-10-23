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
