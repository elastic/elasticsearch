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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.unit.TimeValue;

/**
 * A reference to a cached object that with associated {@code ttl}. The associated {@link Loader} is
 * responsible for loading fresh instances after ttl has expired.
 */
public class CachedReference<T> {

    /**
     * Responsible for loading a fresh instance of the object
     */
    public static interface Loader<T> {

        /**
         * @return the new/fresh instance of the object.
         */
        T load();
    }

    private final Loader<T> loader;
    private final long ttl;
    private T ref;
    private long lastLoadedMillis;


    /**
     * Constructs a new cached reference.
     *
     * @param ttl       Determines the frequency by which a fresh object will be reloaded. A negative or a {@code null} value
     *                  disables the refresh (the object will only be loaded once - lazily). A zero {@code ttl} wll cause this
     *                  reference to always reload and never cache.
     *
     * @param loader    The Loader responsible for loading a fresh instance of the object.
     */
    public CachedReference(TimeValue ttl, Loader<T> loader) {
        this(ttl.millis(), loader);
    }

    /**
     * Constructs a new cached reference.
     *
     * @param ttl       Determines the frequency by which a fresh object will be reloaded (in milliseconds). A negative value
     *                  disables the refresh (the object will only be loaded once - lazily). A zero {@code ttl} wll cause this
     *                  reference to always reload and never cache.
     *
     * @param loader    The Loader responsible for loading a fresh instance of the object.
     */
    public CachedReference(long ttl, Loader<T> loader) {
        this.ttl = ttl;
        this.loader = loader;
    }

    /**
     * @return The referenced object (either previously loaded one, or a freshly loaded one)
     */
    public synchronized T get() {
        if (ref != null && (ttl < 0 || lastLoadedMillis + ttl < System.currentTimeMillis())) {
            assert lastLoadedMillis > 0;
            return ref;
        }
        ref = loader.load();
        lastLoadedMillis = System.currentTimeMillis();
        return ref;
    }
}
