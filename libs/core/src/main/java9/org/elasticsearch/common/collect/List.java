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

import java.util.Collection;

public class List {

    /**
     * Delegates to the Java9 {@code List.of()} method.
     *
     * @param <T> the {@code List}'s element type
     * @return an empty {@code List}
     */
    public static <T> java.util.List<T> of() {
        return java.util.List.of();
    }

    /**
     * Delegates to the Java9 {@code List.of()} method.
     *
     * @param <T> the {@code List}'s element type
     * @param e1  the single element
     * @return a {@code List} containing the specified element
     */
    public static <T> java.util.List<T> of(T e1) {
        return java.util.List.of(e1);
    }

    /**
     * Delegates to the Java9 {@code List.of()} method.
     *
     * @param <T> the {@code List}'s element type
     * @param e1  the single element
     * @return a {@code List} containing the specified element
     */
    public static <T> java.util.List<T> of(T e1, T e2) {
        return java.util.List.of(e1, e2);
    }

    /**
     * Delegates to the Java9 {@code List.of()} method.
     *
     * @param entries the elements to be contained in the list
     * @param <T>     the {@code List}'s element type
     * @return an unmodifiable list containing the specified elements.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> java.util.List<T> of(T... entries) {
        return java.util.List.of(entries);
    }

    /**
     * Delegates to the Java9 {@code List.copyOf()} method.
     *
     * @param <T>  the {@code List}'s element type
     * @param coll a {@code Collection} from which elements are drawn, must be non-null
     * @return a {@code List} containing the elements of the given {@code Collection}
     */
    public static <T> java.util.List<T> copyOf(Collection<? extends T> coll) {
        return java.util.List.copyOf(coll);
    }
}
