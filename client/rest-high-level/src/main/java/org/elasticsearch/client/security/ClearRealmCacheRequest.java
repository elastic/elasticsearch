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
