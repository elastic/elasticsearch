/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
