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
 * Request object to get application privilege(s)
 */
public final class GetPrivilegesRequest implements Validatable {
    private final String applicationName;
    private final String privilegeName;

    public GetPrivilegesRequest(@Nullable final String applicationName, @Nullable final String privilegeNames) {
        if (Strings.hasText(privilegeNames) && Strings.isNullOrEmpty(applicationName)) {
            throw new IllegalArgumentException("privilege cannot be specified when application is missing");
        }
        this.applicationName = applicationName;
        this.privilegeName = privilegeNames;
    }

    /**
     * Constructs a {@link GetPrivilegesRequest} to request all the privileges defined for all applications
     */
    public static GetPrivilegesRequest getAllPrivileges() {
        return new GetPrivilegesRequest(null, null);
    }

    /**
     * Constructs a {@link GetPrivilegesRequest} to request all the privileges defined for the specified {@code applicationName}
     *
     * @param applicationName the name of the application for which the privileges are requested
     */
    public static GetPrivilegesRequest getApplicationPrivileges(String applicationName) {
        if (Strings.isNullOrEmpty(applicationName)) {
            throw new IllegalArgumentException("application name is required");
        }
        return new GetPrivilegesRequest(applicationName, null);
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getPrivilegeName() {
        return privilegeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetPrivilegesRequest that = (GetPrivilegesRequest) o;
        return Objects.equals(applicationName, that.applicationName) &&
            Objects.equals(privilegeName, that.privilegeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationName, privilegeName);
    }
}
