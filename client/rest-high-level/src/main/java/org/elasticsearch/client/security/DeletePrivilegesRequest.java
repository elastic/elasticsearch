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
import org.elasticsearch.common.util.CollectionUtils;

/**
 * A request to delete application privileges
 */
public final class DeletePrivilegesRequest implements Validatable {

    private final String application;
    private final String[] privileges;
    private final RefreshPolicy refreshPolicy;

    /**
     * Creates a new {@link DeletePrivilegesRequest} using the default {@link RefreshPolicy#getDefault()} refresh policy.
     *
     * @param application   the name of the application for which the privileges will be deleted
     * @param privileges    the privileges to delete
     */
    public DeletePrivilegesRequest(String application, String... privileges) {
        this(application, privileges, null);
    }

    /**
     * Creates a new {@link DeletePrivilegesRequest}.
     *
     * @param application   the name of the application for which the privileges will be deleted
     * @param privileges    the privileges to delete
     * @param refreshPolicy the refresh policy {@link RefreshPolicy} for the request, defaults to {@link RefreshPolicy#getDefault()}
     */
    public DeletePrivilegesRequest(String application, String[] privileges, @Nullable RefreshPolicy refreshPolicy) {
        if (Strings.hasText(application) == false) {
            throw new IllegalArgumentException("application name is required");
        }
        if (CollectionUtils.isEmpty(privileges)) {
            throw new IllegalArgumentException("privileges are required");
        }
        this.application = application;
        this.privileges = privileges;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
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
