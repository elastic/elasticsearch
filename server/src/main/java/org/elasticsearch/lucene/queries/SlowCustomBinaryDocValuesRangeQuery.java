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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * A query for matching any binary doc value (per document) that falls in an inclusive {@link BytesRef} range.
 * Values are compared with {@link BytesRef#compareTo}, which matches {@link InetAddressPoint} sort order for IP
 * fields.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesRangeQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef lower;
    private final BytesRef upper;

    public SlowCustomBinaryDocValuesRangeQuery(String fieldName, BytesRef lower, BytesRef upper) {
        super(fieldName, rangeMatcher(Objects.requireNonNull(lower), Objects.requireNonNull(upper)));
        assert lower.compareTo(upper) <= 0;
        this.lower = lower;
        this.upper = upper;
    }

    private static Predicate<BytesRef> rangeMatcher(BytesRef lower, BytesRef upper) {
        return value -> lower.compareTo(value) <= 0 && upper.compareTo(value) >= 0;
    }

    @Override
    protected float matchCost() {
        return 20; // two comparisons per candidate value
    }

    @Override
    public String toString(String field) {
        return "SlowCustomBinaryDocValuesRangeQuery(fieldName="
            + field
            + ",lower="
            + InetAddresses.toAddrString(InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(lower))))
            + ",upper="
            + InetAddresses.toAddrString(InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(upper))))
            + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesRangeQuery that = (SlowCustomBinaryDocValuesRangeQuery) o;
        return Objects.equals(fieldName, that.fieldName) && lower.bytesEquals(that.lower) && upper.bytesEquals(that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, lower, upper);
    }
}
