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

package org.elasticsearch.common.collect;

import com.google.common.collect.ForwardingSet;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Set;

/**
 * {@link Set} implementation based on {@link CopyOnWriteHashMap}.
 * Null values are not supported.
 */
public class CopyOnWriteHashSet<T> extends ForwardingSet<T> {

    /**
     * Return a copy of the provided set.
     */
    public static <T> CopyOnWriteHashSet<T> copyOf(Collection<? extends T> set) {
        if (set instanceof CopyOnWriteHashSet) {
            // no need to copy in that case
            @SuppressWarnings("unchecked")
            final CopyOnWriteHashSet<T> cowSet = (CopyOnWriteHashSet<T>) set;
            return cowSet;
        } else {
            return new CopyOnWriteHashSet<T>().copyAndAddAll(set);
        }
    }

    private final CopyOnWriteHashMap<T, Boolean> map;

    /** Create a new empty set. */
    public CopyOnWriteHashSet() {
        this(new CopyOnWriteHashMap<T, Boolean>());
    }

    private CopyOnWriteHashSet(CopyOnWriteHashMap<T, Boolean> map) {
        this.map = map;
    }

    @Override
    protected Set<T> delegate() {
        return map.keySet();
    }

    /**
     * Copy the current set and return a copy that contains or replaces <code>entry</code>.
     */
    public CopyOnWriteHashSet<T> copyAndAdd(T entry) {
        return new CopyOnWriteHashSet<>(map.copyAndPut(entry, true));
    }

    /**
     * Copy the current set and return a copy that is the union of the current
     * set and <code>entries</code>, potentially replacing existing entries in
     * case of equality.
     */
    public CopyOnWriteHashSet<T> copyAndAddAll(Collection<? extends T> entries) {
        CopyOnWriteHashMap<T, Boolean> updated = this.map.copyAndPutAll(entries.stream().map(
                p -> new AbstractMap.SimpleImmutableEntry<>(p, true)
        ));
        return new CopyOnWriteHashSet<>(updated);
    }

    /**
     * Copy the current set and return a copy that removes <code>entry</code>
     * if it exists.
     */
    public CopyOnWriteHashSet<T> copyAndRemove(Object entry) {
        final CopyOnWriteHashMap<T, Boolean> updated = map.copyAndRemove(entry);
        if (updated == map) {
            return this;
        } else {
            return new CopyOnWriteHashSet<>(updated);
        }
    }

    /**
     * Copy the current set and return a copy that is the difference of the current
     * set and <code>entries</code>.
     */
    public CopyOnWriteHashSet<T> copyAndRemoveAll(Collection<?> entries) {
        CopyOnWriteHashMap<T, Boolean> updated = this.map.copyAndRemoveAll(entries);
        if (updated == map) {
            return this;
        } else {
            return new CopyOnWriteHashSet<>(updated);
        }
    }

}
