/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class List {

    /**
     * Returns an unmodifiable list containing zero elements.
     *
     * @param <T> the {@code List}'s element type
     * @return an empty {@code List}
     */
    public static <T> java.util.List<T> of() {
        return Collections.emptyList();
    }

    /**
     * Returns an unmodifiable list containing one element.
     *
     * @param <T> the {@code List}'s element type
     * @param e1  the single element
     * @return a {@code List} containing the specified element
     */
    public static <T> java.util.List<T> of(T e1) {
        return Collections.singletonList(e1);
    }

    /**
     * Returns an unmodifiable list containing two elements.
     *
     * @param <T> the {@code List}'s element type
     * @param e1 the first element
     * @param e2 the second element
     * @return a {@code List} containing the specified element
     */
    @SuppressWarnings("unchecked")
    public static <T> java.util.List<T> of(T e1, T e2) {
        return List.of((T[]) new Object[] { e1, e2 });
    }

    /**
     * Returns an unmodifiable list containing an arbitrary number of elements.
     *
     * @param entries the elements to be contained in the list
     * @param <T>     the {@code List}'s element type
     * @return an unmodifiable list containing the specified elements.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> java.util.List<T> of(T... entries) {
        switch (entries.length) {
            case 0:
                return List.of();
            case 1:
                return List.of(entries[0]);
            default:
                return Collections.unmodifiableList(Arrays.asList(entries));
        }
    }

    /**
     * Returns an unmodifiable {@code List} containing the elements of the given {@code Collection} in iteration order.
     *
     * @param <T>  the {@code List}'s element type
     * @param coll a {@code Collection} from which elements are drawn, must be non-null
     * @return a {@code List} containing the elements of the given {@code Collection}
     */
    public static <T> java.util.List<T> copyOf(Collection<? extends T> coll) {
        switch (coll.size()) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(coll.iterator().next());
            default:
                return Collections.unmodifiableList(new ArrayList<>(coll));
        }
    }
}
