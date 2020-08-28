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

import java.util.Arrays;
import java.util.Objects;

/**
 * Request object to get application privilege(s)
 */
public final class GetPrivilegesRequest implements Validatable {
    private final String applicationName;
    private final String[] privilegeNames;

    public GetPrivilegesRequest(@Nullable final String applicationName, @Nullable final String... privilegeNames) {
        if ((CollectionUtils.isEmpty(privilegeNames) == false) && Strings.isNullOrEmpty(applicationName)) {
            throw new IllegalArgumentException("privilege cannot be specified when application is missing");
        }
        this.applicationName = applicationName;
        this.privilegeNames = privilegeNames;
    }

    /**
     * Constructs a {@link GetPrivilegesRequest} to request all the privileges defined for all applications
     */
    public static GetPrivilegesRequest getAllPrivileges() {
        return new GetPrivilegesRequest(null);
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
        return new GetPrivilegesRequest(applicationName);
    }

    /**
     * @return the name of the application for which to return certain privileges
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * @return an array of privilege names to return or null if all should be returned
     */
    public String[] getPrivilegeNames() {
        return privilegeNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetPrivilegesRequest that = (GetPrivilegesRequest) o;
        return Objects.equals(applicationName, that.applicationName) &&
            Arrays.equals(privilegeNames, that.privilegeNames);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(applicationName);
        result = 31 * result + Arrays.hashCode(privilegeNames);
        return result;
    }
}
