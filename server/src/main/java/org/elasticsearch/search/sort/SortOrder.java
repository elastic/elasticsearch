/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Locale;

/**
 * A sorting order.
 *
 *
 */
public enum SortOrder implements Writeable {
    /**
     * Ascending order.
     */
    ASC {
        @Override
        public String toString() {
            return "asc";
        }

        @Override
        public int reverseMul() {
            return 1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate;
        }
    },
    /**
     * Descending order.
     */
    DESC {
        @Override
        public String toString() {
            return "desc";
        }

        @Override
        public int reverseMul() {
            return -1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate.reversed();
        }
    };

    public static SortOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(SortOrder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SortOrder fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    /**
     * -1 if the sort is reversed from the standard comparators, 1 otherwise.
     */
    public abstract int reverseMul();

    /**
     * Wrap a comparator in one for this direction.
     */
    public abstract <T> Comparator<T> wrap(Comparator<T> delegate);
}
