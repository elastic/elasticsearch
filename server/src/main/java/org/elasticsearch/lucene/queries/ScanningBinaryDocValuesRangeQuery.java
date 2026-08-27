/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A query for matching any binary doc value (per document) that falls in an inclusive {@link BytesRef} range.
 * Values are compared with {@link BytesRef#compareTo}, which matches {@link InetAddressPoint} sort order for IP
 * fields.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 * <p>
 * When the lower and upper bound are identical, {@link #rewrite(IndexSearcher)} returns a
 * {@link ScanningBinaryDocValuesTermQuery}.
 */
public final class ScanningBinaryDocValuesRangeQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef lower;
    private final BytesRef upper;
    private final boolean includeLower;
    private final boolean includeUpper;

    public ScanningBinaryDocValuesRangeQuery(String fieldName, BytesRef lower, BytesRef upper, BinaryFormat binaryFormat) {
        this(fieldName, lower, upper, true, true, binaryFormat);
    }

    public ScanningBinaryDocValuesRangeQuery(
        String fieldName,
        BytesRef lower,
        BytesRef upper,
        boolean includeLower,
        boolean includeUpper,
        BinaryFormat binaryFormat
    ) {
        super(fieldName, rangeMatcher(lower, upper, includeLower, includeUpper), binaryFormat);
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    private static Predicate<BytesRef> rangeMatcher(BytesRef lower, BytesRef upper, boolean includeLower, boolean includeUpper) {
        return value -> isAboveLowerBound(value, lower, includeLower) && isBelowUpperBound(value, upper, includeUpper);
    }

    private static boolean isAboveLowerBound(BytesRef value, BytesRef lower, boolean includeLower) {
        if (lower == null) {
            return true;
        }
        int cmp = value.compareTo(lower);
        return includeLower ? cmp >= 0 : cmp > 0;
    }

    private static boolean isBelowUpperBound(BytesRef value, BytesRef upper, boolean includeUpper) {
        if (upper == null) {
            return true;
        }
        int cmp = value.compareTo(upper);
        return includeUpper ? cmp <= 0 : cmp < 0;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (lower != null && upper != null && includeLower && includeUpper && lower.bytesEquals(upper)) {
            return new ScanningBinaryDocValuesTermQuery(fieldName, lower, binaryFormat);
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    protected float matchCost() {
        return 20;
    }

    @Override
    public String toString(String field) {
        return "ScanningBinaryDocValuesRangeQuery(fieldName=" + fieldName + ",lower=" + lower + ",upper=" + upper + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesRangeQuery that = (ScanningBinaryDocValuesRangeQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(lower, that.lower)
            && Objects.equals(upper, that.upper)
            && includeLower == that.includeLower
            && includeUpper == that.includeUpper
            && binaryFormat == that.binaryFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, lower, upper, includeLower, includeUpper, binaryFormat);
    }
}
