/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.document;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * A range field for binary encoded ranges
 */
public final class BinaryRange extends Field {
    /** The number of bytes per dimension, use {@link InetAddressPoint#BYTES} as max, because that is maximum we need to support */
    public static final int BYTES = InetAddressPoint.BYTES;

    private static final FieldType TYPE;
    static {
        TYPE = new FieldType();
        TYPE.setDimensions(2, BYTES);
        TYPE.freeze();
    }

    /**
     * Create a new BinaryRange from a provided encoded binary range
     * @param name              field name. must not be null.
     * @param encodedRange      Encoded range
     */
    public BinaryRange(String name, byte[] encodedRange) {
        super(name, TYPE);
        if (encodedRange.length != BYTES * 2) {
            throw new IllegalArgumentException("Unexpected encoded range length [" + encodedRange.length + "]");
        }
        fieldsData = new BytesRef(encodedRange);
    }

    /**
     * Create a query for matching indexed ip ranges that {@code INTERSECT} the defined range.
     * @param field         field name. must not be null.
     * @param encodedRange  Encoded range
     * @return query for matching intersecting encoded ranges (overlap, within, crosses, or contains)
     * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
     */
    public static Query newIntersectsQuery(String field, byte[] encodedRange) {
        return newRelationQuery(field, encodedRange, RangeFieldQuery.QueryType.INTERSECTS);
    }

    static Query newRelationQuery(String field, byte[] encodedRange, RangeFieldQuery.QueryType relation) {
        return new RangeFieldQuery(field, encodedRange, 1, relation) {
            @Override
            protected String toString(byte[] ranges, int dimension) {
                return "[" + new BytesRef(ranges, 0, BYTES) + " TO " + new BytesRef(ranges, BYTES, BYTES) + "]";
            }
        };
    }

}
