/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class Set {

    /**
     * Returns an unmodifiable set containing zero elements.
     *
     * @param <T> the {@code Set}'s element type
     * @return an empty {@code Set}
     */
    public static <T> java.util.Set<T> of() {
        return Collections.emptySet();
    }

    /**
     * Returns an unmodifiable set containing one element.
     *
     * @param <T> the {@code Set}'s element type
     * @param e1  the single element
     * @return a {@code Set} containing the specified element
     */
    public static <T> java.util.Set<T> of(T e1) {
        return Collections.singleton(e1);
    }

    /**
     * Returns an unmodifiable set containing two elements.
     *
     * @param <T> the {@code Set}'s element type
     * @param e1 the first element
     * @param e2 the second element
     * @return a {@code Set} containing the specified element
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.Set<T> of(T e1, T e2) {
        return Set.of((T[]) new Object[]{e1, e2});
    }

    /**
     * Returns an unmodifiable set containing an arbitrary number of elements.
     *
     * @param entries the elements to be contained in the set
     * @param <T>     the {@code Set}'s element type
     * @return an unmodifiable set containing the specified elements.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> java.util.Set<T> of(T... entries) {
        switch (entries.length) {
            case 0:
                return Set.of();
            case 1:
                return Set.of(entries[0]);
            default:
                return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(entries)));
        }
    }

    /**
     * Returns an unmodifiable {@code Set} containing the elements of the given Collection.
     *
     * @param <T> the {@code Set}'s element type
     * @param coll a {@code Collection} from which elements are drawn, must be non-null
     * @return a {@code Set} containing the elements of the given {@code Collection}
     * @throws NullPointerException if coll is null, or if it contains any nulls
     * @since 10
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.Set<T> copyOf(Collection<? extends T> coll) {
        return (java.util.Set<T>) Set.of(new HashSet<>(coll).toArray());
    }
}
