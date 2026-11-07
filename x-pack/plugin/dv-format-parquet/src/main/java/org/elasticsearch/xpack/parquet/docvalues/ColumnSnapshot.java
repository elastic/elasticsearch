/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.Arrays;

/**
 * What the reader needs to know about one column of a file piece, taken from the Parquet footer when the piece is attached: the
 * type of the column and where the chunk of each row group of the piece lies in the file. It lets a segment open, and a reader
 * decode, without a request for the footer.
 *
 * @param path the dotted path of the column
 * @param physicalType the Parquet physical type (the value of {@code org.apache.parquet.format.Type})
 * @param convertedType the Parquet converted type, or {@code -1} if the column has none
 * @param required whether the column is {@code REQUIRED}, so that no value is missing
 * @param codec the compression codec of the chunks (the value of {@code org.apache.parquet.format.CompressionCodec})
 * @param offsets for each row group, the file offset of the first byte of the column chunk, which is its dictionary page if it has one
 * @param compressedSizes for each row group, the size of the chunk in the file
 * @param uncompressedSizes for each row group, the size of the chunk after decompression
 * @param numValues for each row group, the number of values of the chunk
 * @param minValues for each row group, the smallest value of the chunk as the footer says (before any conversion), or
 *        {@link #UNKNOWN_MIN} if the footer has no usable statistic
 * @param maxValues for each row group, the largest value of the chunk, or {@link #UNKNOWN_MAX}
 * @param nullCounts for each row group, the number of missing values, or {@link #UNKNOWN_NULL_COUNT} if the footer does not say
 */
public record ColumnSnapshot(
    String path,
    int physicalType,
    int convertedType,
    boolean required,
    int codec,
    long[] offsets,
    long[] compressedSizes,
    long[] uncompressedSizes,
    long[] numValues,
    long[] minValues,
    long[] maxValues,
    long[] nullCounts
) {

    public static final long UNKNOWN_MIN = Long.MIN_VALUE;
    public static final long UNKNOWN_MAX = Long.MAX_VALUE;
    public static final long UNKNOWN_NULL_COUNT = -1L;

    /** A snapshot without statistics, so with the widest range and unknown null counts for every row group. */
    public ColumnSnapshot(
        String path,
        int physicalType,
        int convertedType,
        boolean required,
        int codec,
        long[] offsets,
        long[] compressedSizes,
        long[] uncompressedSizes,
        long[] numValues
    ) {
        this(
            path,
            physicalType,
            convertedType,
            required,
            codec,
            offsets,
            compressedSizes,
            uncompressedSizes,
            numValues,
            filled(offsets.length, UNKNOWN_MIN),
            filled(offsets.length, UNKNOWN_MAX),
            filled(offsets.length, UNKNOWN_NULL_COUNT)
        );
    }

    private static long[] filled(int length, long value) {
        long[] array = new long[length];
        Arrays.fill(array, value);
        return array;
    }

    /**
     * The number of values that a row group has, which is a row count for a required column and the row count minus the null count for
     * an optional one, or {@code -1} if that is not known.
     */
    public long valueCount(int rowGroup, int rows) {
        if (required) {
            return rows;
        }
        return nullCounts[rowGroup] == UNKNOWN_NULL_COUNT ? -1 : rows - nullCounts[rowGroup];
    }

    /** Whether {@link #valueCount} is known for every row group, which a doc values skipper needs. */
    public boolean hasExactValueCounts() {
        if (required) {
            return true;
        }
        for (long nullCount : nullCounts) {
            if (nullCount == UNKNOWN_NULL_COUNT) {
                return false;
            }
        }
        return true;
    }

    public int rowGroups() {
        return offsets.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ColumnSnapshot other
            && path.equals(other.path)
            && physicalType == other.physicalType
            && convertedType == other.convertedType
            && required == other.required
            && codec == other.codec
            && Arrays.equals(offsets, other.offsets)
            && Arrays.equals(compressedSizes, other.compressedSizes)
            && Arrays.equals(uncompressedSizes, other.uncompressedSizes)
            && Arrays.equals(numValues, other.numValues)
            && Arrays.equals(minValues, other.minValues)
            && Arrays.equals(maxValues, other.maxValues)
            && Arrays.equals(nullCounts, other.nullCounts);
    }

    @Override
    public int hashCode() {
        return path.hashCode() * 31 + Arrays.hashCode(offsets);
    }
}
