/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.hadoop.thirdparty.com.google.common.primitives;

/**
 * Minimal stub replacing the {@code hadoop-shaded-guava} jar (3.5 MB).
 * <p>
 * The original class lives in Google Guava ({@code com.google.common.primitives.UnsignedBytes},
 * Apache License 2.0) and is shaded by the Hadoop project into the
 * {@code org.apache.hadoop.thirdparty} package namespace.
 * {@code org.apache.hadoop.io.FastByteComparisons$LexicographicalComparerHolder$UnsafeComparer}
 * (in {@code hadoop-common}) calls {@code UnsignedBytes.compare(byte, byte)} as the
 * fall-through comparison when the unsafe fast path encounters differing bytes.
 * <p>
 * Both this implementation and the Guava original use {@code Byte.toUnsignedInt}
 * (the canonical way to compare bytes as unsigned in Java). This is an independent
 * reimplementation; no code was copied from Guava.
 */
public final class UnsignedBytes {

    private UnsignedBytes() {}

    public static int compare(byte a, byte b) {
        return Byte.toUnsignedInt(a) - Byte.toUnsignedInt(b);
    }
}
