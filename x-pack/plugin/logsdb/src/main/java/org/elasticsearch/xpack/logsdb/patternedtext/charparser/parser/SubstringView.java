/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

/**
 * Provides a substring view based on a given CharSequence, allowing for substring functionality without allocating new string objects.
 * The substring's first index is inclusive and the last index is exclusive.
 */
public class SubstringView implements CharSequence {
    private CharSequence source;
    private int start;
    private int end;
    private int hashCode;
    private boolean hashComputed;

    public SubstringView(CharSequence source) {
        this(source, 0, source.length());
    }

    public SubstringView(CharSequence source, int start, int end) {
        if (source == null) {
            throw new IllegalArgumentException("Source cannot be null");
        }
        if (start < 0 || end > source.length() || start > end) {
            throw new IndexOutOfBoundsException("Invalid start or end index");
        }
        this.source = source;
        this.start = start;
        this.end = end;
    }

    public void set(CharSequence source, int start, int end) {
        this.source = source;
        set(start, end);
    }

    public void set(int start, int end) {
        this.start = start;
        this.end = end;
        this.hashCode = 0;
        this.hashComputed = false;
    }

    @Override
    public int length() {
        return end - start;
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length()) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + index);
        }
        return source.charAt(start + index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException("Not supported to avoid allocations. This IS the subsequence.");
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(length());
        for (int i = start; i < end; i++) {
            sb.append(source.charAt(i));
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        if (hashComputed == false) {
            int h = 1;
            for (int i = start; i < end; i++) {
                h = 31 * h + source.charAt(i);
            }
            hashCode = h;
            hashComputed = true;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof SubstringView other) {
            if (length() != other.length()) return false;
            for (int i = 0; i < length(); i++) {
                if (charAt(i) != other.charAt(i)) return false;
            }
            return true;
        }
        return false;
    }
}
