/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.util.stream.IntStream;

/**
 * A CharSequence that provides a subsequence of another CharSequence without allocating a new backing array (as String does)
 */
class CharSubSequence implements CharSequence {
    private final CharSequence wrapped;
    private final int startOffset;    // inclusive
    private final int endOffset;      // exclusive

    CharSubSequence(CharSequence wrapped, int startOffset, int endOffset) {
        if (startOffset < 0) throw new IllegalArgumentException();
        if (endOffset > wrapped.length()) throw new IllegalArgumentException();
        if (endOffset < startOffset) throw new IllegalArgumentException();

        this.wrapped = wrapped;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public int length() {
        return endOffset - startOffset;
    }

    @Override
    public char charAt(int index) {
        int adjustedIndex = index + startOffset;
        if (adjustedIndex < startOffset || adjustedIndex >= endOffset) throw new IndexOutOfBoundsException(index);
        return wrapped.charAt(adjustedIndex);
    }

    @Override
    public boolean isEmpty() {
        return startOffset == endOffset;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        int adjustedStart = start + startOffset;
        int adjustedEnd = end + startOffset;
        if (adjustedStart < startOffset) throw new IndexOutOfBoundsException(start);
        if (adjustedEnd > endOffset) throw new IndexOutOfBoundsException(end);
        if (adjustedStart > adjustedEnd) throw new IndexOutOfBoundsException();

        return wrapped.subSequence(adjustedStart, adjustedEnd);
    }

    @Override
    public IntStream chars() {
        return wrapped.chars().skip(startOffset).limit(endOffset - startOffset);
    }

    @Override
    public String toString() {
        return wrapped.subSequence(startOffset, endOffset).toString();
    }
}
