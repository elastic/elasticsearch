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
 * A request to delete application privileges
 */
public final class DeletePrivilegesRequest implements Validatable {

    private final String application;
    private final String[] privileges;
    private final RefreshPolicy refreshPolicy;

    public DeletePrivilegesRequest(String application, String... privileges) {
        this(application, privileges, RefreshPolicy.IMMEDIATE);
    }

    public DeletePrivilegesRequest(String application, String[] privileges, RefreshPolicy refreshPolicy) {
        this.application = Objects.requireNonNull(application, "application is required");
        this.privileges = Objects.requireNonNull(privileges, "privileges are required");
        this.refreshPolicy =  Objects.requireNonNull(refreshPolicy, "refresh policy is required");
    }

    public String getApplication() {
        return application;
    }

    public String[] getPrivileges() {
        return privileges;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}
