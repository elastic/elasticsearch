/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.Collection;

public class Set {

    /**
     * Delegates to the Java9 {@code Set.of()} method.
     *
     * @param <T> the {@code Set}'s element type
     * @return an empty {@code Set}
     */
    public static <T> java.util.Set<T> of() {
        return java.util.Set.of();
    }

    /**
     * Delegates to the Java9 {@code Set.of()} method.
     *
     * @param <T> the {@code Set}'s element type
     * @param e1  the single element
     * @return a {@code Set} containing the specified element
     */
    public static <T> java.util.Set<T> of(T e1) {
        return java.util.Set.of(e1);
    }

    /**
     * Delegates to the Java9 {@code Set.of()} method.
     *
     * @param <T> the {@code Set}'s element type
     * @param e1 the first element
     * @param e2 the second element
     * @return a {@code Set} containing the specified element
     */
    public static <T> java.util.Set<T> of(T e1, T e2) {
        return java.util.Set.of(e1, e2);
    }

    /**
     * Delegates to the Java9 {@code Set.of()} method.
     *
     * @param entries the elements to be contained in the set
     * @param <T>     the {@code Set}'s element type
     * @return an unmodifiable set containing the specified elements.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> java.util.Set<T> of(T... entries) {
        return java.util.Set.of(entries);
    }

    /**
     * Delegates to the Java10 {@code Set.copyOf} method.
     *
     * @param <T>  the {@code Set}'s element type
     * @param coll a {@code Collection} from which elements are drawn, must be non-null
     * @return a {@code Set} containing the elements of the given {@code Collection}
     */
    public static <T> java.util.Set<T> copyOf(Collection<? extends T> coll) {
        return java.util.Set.copyOf(coll);
    }
}
