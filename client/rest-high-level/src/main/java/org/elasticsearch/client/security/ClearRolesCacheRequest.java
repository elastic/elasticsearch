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
 * The request used to clear the cache for native roles stored in an index.
 */
public final class ClearRolesCacheRequest implements Validatable {

    private final String[] names;

    /**
     * Sets the roles for which caches will be evicted. When not set all the roles will be evicted from the cache.
     *
     * @param names    The role names
     */
    public ClearRolesCacheRequest(String... names) {
        this.names = names;
    }

    /**
     * @return an array of role names that will have the cache evicted or <code>null</code> if all
     */
    public String[] names() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClearRolesCacheRequest that = (ClearRolesCacheRequest) o;
        return Arrays.equals(names, that.names);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(names);
    }
}
