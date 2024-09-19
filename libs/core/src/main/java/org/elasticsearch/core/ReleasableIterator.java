/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.Iterator;
import java.util.Objects;

/**
 * An {@link Iterator} with state that must be {@link #close() released}.
 */
public interface ReleasableIterator<T> extends Releasable, Iterator<T> {
    /**
     * Returns a single element iterator over the supplied value.
     */
    static <T extends Releasable> ReleasableIterator<T> single(T element) {
        return new ReleasableIterator<>() {
            private T value = Objects.requireNonNull(element);

            @Override
            public boolean hasNext() {
                return value != null;
            }

            @Override
            public T next() {
                final T res = value;
                value = null;
                return res;
            }

            @Override
            public void close() {
                Releasables.close(value);
            }

            @Override
            public String toString() {
                return "ReleasableIterator[" + value + "]";
            }

        };
    }

    /**
     * Returns an empty iterator over the supplied value.
     */
    static <T extends Releasable> ReleasableIterator<T> empty() {
        return new ReleasableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                assert false : "hasNext is always false so next should never be called";
                return null;
            }

            @Override
            public void close() {}

            @Override
            public String toString() {
                return "ReleasableIterator[<empty>]";
            }
        };
    }
}
