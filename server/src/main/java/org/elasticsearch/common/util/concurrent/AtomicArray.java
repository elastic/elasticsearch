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

import org.elasticsearch.ElasticsearchGenerationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A list backed by an {@link AtomicReferenceArray} with potential null values, easily allowing
 * to get the concrete values as a list using {@link #asList()}.
 */
public class AtomicArray<E> {
    private final AtomicReferenceArray<E> array;
    private volatile List<E> nonNullList;

    public AtomicArray(int size) {
        array = new AtomicReferenceArray<>(size);
    }

    /**
     * The size of the expected results, including potential null values.
     */
    public int length() {
        return array.length();
    }

    /**
     * Sets the element at position {@code i} to the given value.
     *
     * @param i     the index
     * @param value the new value
     */
    public void set(int i, E value) {
        array.set(i, value);
        if (nonNullList != null) { // read first, lighter, and most times it will be null...
            nonNullList = null;
        }
    }

    public final void setOnce(int i, E value) {
        if (array.compareAndSet(i, null, value) == false) {
            throw new IllegalStateException("index [" + i + "] has already been set");
        }
        if (nonNullList != null) { // read first, lighter, and most times it will be null...
            nonNullList = null;
        }
    }

    /**
     * Gets the current value at position {@code i}.
     *
     * @param i the index
     * @return the current value
     */
    public E get(int i) {
        return array.get(i);
    }

    /**
     * Returns the it as a non null list.
     */
    public List<E> asList() {
        if (nonNullList == null) {
            if (array == null || array.length() == 0) {
                nonNullList = Collections.emptyList();
            } else {
                List<E> list = new ArrayList<>(array.length());
                for (int i = 0; i < array.length(); i++) {
                    E e = array.get(i);
                    if (e != null) {
                        list.add(e);
                    }
                }
                nonNullList = list;
            }
        }
        return nonNullList;
    }

    /**
     * Copies the content of the underlying atomic array to a normal one.
     */
    public E[] toArray(E[] a) {
        if (a.length != array.length()) {
            throw new ElasticsearchGenerationException("AtomicArrays can only be copied to arrays of the same size");
        }
        for (int i = 0; i < array.length(); i++) {
            a[i] = array.get(i);
        }
        return a;
    }
}
