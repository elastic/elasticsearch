/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Integers {
    private Integers() {
    }

    public static Iterable<Integer> asIterable(int[] values) {
        Objects.requireNonNull(values);
        return () -> new Iterator<Integer>() {
            private int position = 0;
            @Override
            public boolean hasNext() {
                return position < values.length;
            }

            @Override
            public Integer next() {
                if (position < values.length) {
                    return values[position++];
                } else {
                    throw new NoSuchElementException("position: " + position + ", length: " + values.length);
                }
            }
        };
    }

    public static boolean contains(int[] values, final int value) {
        return Arrays.stream(values).anyMatch(v -> v == value);
    }
}
