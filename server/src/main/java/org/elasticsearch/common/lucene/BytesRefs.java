/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.StringSorter;

public class BytesRefs {

    /**
     * Converts a value to a string, taking special care if its a {@link BytesRef} to call
     * {@link org.apache.lucene.util.BytesRef#utf8ToString()}.
     */
    public static String toString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    /**
     * Converts an object value to BytesRef.
     */
    public static BytesRef toBytesRef(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return (BytesRef) value;
        }
        return new BytesRef(value.toString());
    }

    public static BytesRef toBytesRef(Object value, BytesRefBuilder spare) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return (BytesRef) value;
        }
        spare.copyChars(value.toString());
        return spare.get();
    }

    /**
     * Sorts {@code refs} in-place using Lucene's radix sort (BytesRef natural order).
     */
    public static void radixSort(BytesRef[] refs) {
        new StringSorter(BytesRefComparator.NATURAL) {
            @Override
            protected void get(BytesRefBuilder builder, BytesRef result, int i) {
                BytesRef t = refs[i];
                result.bytes = t.bytes;
                result.offset = t.offset;
                result.length = t.length;
            }

            @Override
            protected void swap(int i, int j) {
                BytesRef tmp = refs[i];
                refs[i] = refs[j];
                refs[j] = tmp;
            }
        }.sort(0, refs.length);
    }

    /**
     * Returns true if {@code refs} is already sorted in BytesRef natural order.
     */
    public static boolean isSorted(BytesRef[] refs) {
        for (int i = 1; i < refs.length; i++) {
            if (refs[i - 1].compareTo(refs[i]) > 0) return false;
        }
        return true;
    }
}
