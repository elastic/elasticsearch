/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A view into a row.
 * Offers access to the data but it shouldn't be held since it is not a data container.
 */
public interface RowView extends Iterable<Object> {
    /**
     * Number of columns in this row.
     */
    int columnCount();

    Object column(int index);

    default <T> T column(int index, Class<T> type) {
        return type.cast(column(index));
    }

    @Override
    default void forEach(Consumer<? super Object> action) {
        forEachColumn(action::accept);
    }

    default void forEachColumn(Consumer<? super Object> action) {
        Objects.requireNonNull(action);
        int rowSize = columnCount();
        for (int i = 0; i < rowSize; i++) {
            action.accept(column(i));
        }
    }

    @Override
    default Iterator<Object> iterator() {
        return new Iterator<Object>() {
            private int pos = 0;
            private final int rowSize = columnCount();

            @Override
            public boolean hasNext() {
                return pos < rowSize;
            }

            @Override
            public Object next() {
                if (pos >= rowSize) {
                    throw new NoSuchElementException();
                }
                return column(pos++);
            }
        };
    }
}
