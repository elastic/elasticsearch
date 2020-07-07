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

import java.util.Arrays;

/**
 * The request used to clear the cache for native application privileges stored in an index.
 */
public final class ClearPrivilegesCacheRequest implements Validatable {

    private final String[] applications;

    /**
     * Sets the applications for which caches will be evicted. When not set all privileges will be evicted from the cache.
     *
     * @param applications    The application names
     */
    public ClearPrivilegesCacheRequest(String... applications) {
        this.applications = applications;
    }

    /**
     * @return an array of application names that will have the cache evicted or <code>null</code> if all
     */
    public String[] applications() {
        return applications;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClearPrivilegesCacheRequest that = (ClearPrivilegesCacheRequest) o;
        return Arrays.equals(applications, that.applications);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(applications);
    }
}
