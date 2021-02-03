/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
