/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Request for clearing the cache of one or more realms
 */
public final class ClearRealmCacheRequest implements Validatable {

    private final List<String> realms;
    private final List<String> usernames;

    /**
     * Create a new request to clear cache of realms
     * @param realms the realms to clear the cache of. Must not be {@code null}. An empty list
     *               indicates that all realms should have their caches cleared.
     * @param usernames the usernames to clear the cache of. Must not be {@code null}. An empty
     *                  list indicates that every user in the listed realms should have their cache
     *                  cleared.
     */
    public ClearRealmCacheRequest(List<String> realms, List<String> usernames) {
        this.realms = Collections.unmodifiableList(Objects.requireNonNull(realms, "the realms list must not be null"));
        this.usernames = Collections.unmodifiableList(Objects.requireNonNull(usernames, "usernames list must no be null"));
    }

    public List<String> getRealms() {
        return realms;
    }

    public List<String> getUsernames() {
        return usernames;
    }
}
