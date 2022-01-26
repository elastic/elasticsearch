/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

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
        return Set.of((T[]) new Object[] { e1, e2 });
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
                return of();
            case 1:
                return of(entries[0]);
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
    public static <T> java.util.Set<T> copyOf(Collection<? extends T> coll) {
        switch (coll.size()) {
            case 0:
                return Collections.emptySet();
            case 1:
                return Collections.singleton(coll.iterator().next());
            default:
                return Collections.unmodifiableSet(new HashSet<>(coll));
        }
    }
}
