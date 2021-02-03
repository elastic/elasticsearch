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
 * The request used to clear the API key cache.
 */
public final class ClearApiKeyCacheRequest implements Validatable {

    private final String[] ids;

    /**
     * @param ids      An array of API Key ids to be cleared from the specified cache.
     *                 If not specified, all entries will be cleared.
     */
    private ClearApiKeyCacheRequest(String... ids) {
        this.ids = ids;
    }

    public static ClearApiKeyCacheRequest clearAll() {
        return new ClearApiKeyCacheRequest();
    }

    public static ClearApiKeyCacheRequest clearById(String ... ids) {
        if (ids.length == 0) {
            throw new IllegalArgumentException("Ids cannot be empty");
        }
        return new ClearApiKeyCacheRequest(ids);
     }

    /**
     * @return an array of key names that will be evicted
     */
    public String[] ids() {
        return ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClearApiKeyCacheRequest that = (ClearApiKeyCacheRequest) o;
        return Arrays.equals(ids, that.ids);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(ids);
    }
}
