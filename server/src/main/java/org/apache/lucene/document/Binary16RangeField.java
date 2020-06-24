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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;

public class Binary16RangeField extends Field {

    public static final int BYTES = 16;

    private static final FieldType TYPE;
    static {
        TYPE = new FieldType();
        TYPE.setDimensions(2, BYTES);
        TYPE.freeze();
    }

    public Binary16RangeField(String name, final BytesRef min, final BytesRef max) {
        super(name, TYPE);
        setRangeValues(min, max);
    }

    /**
     * Change (or set) the min/max values of the field.
     * @param min range min value
     * @param max range max value
     */
    public void setRangeValues(BytesRef min, BytesRef max) {
        final byte[] bytes;
        if (fieldsData == null) {
            bytes = new byte[BYTES * 2];
            fieldsData = new BytesRef(bytes);
        } else {
            bytes = ((BytesRef) fieldsData).bytes;
        }
        encode(min, max, bytes);
    }

    /** encode the min/max range into the provided byte array */
    private static void encode(final BytesRef min, final BytesRef max, final byte[] bytes) {
        if (FutureArrays.compareUnsigned(min.bytes, 0, BYTES, max.bytes, 0, BYTES) > 0) {
            throw new IllegalArgumentException("min value cannot be greater than max value for version field");
        }
        System.arraycopy(min.bytes, 0 + min.offset, bytes, 0, BYTES);
        System.arraycopy(max.bytes, 0 + max.offset, bytes, BYTES, BYTES);
    }

    /** encode the min/max range and return the byte array */
    private static byte[] encode(BytesRef min, BytesRef max) {
      byte[] b = new byte[BYTES*2];
      encode(min, max, b);
      return b;
    }

    public static Query newIntersectsQuery(String field, BytesRef from, BytesRef to, Query dvQuery) {
        return newRelationQuery(field, from, to, RangeFieldQuery.QueryType.INTERSECTS, dvQuery);
    }

    public static Query newWithinQuery(String field, BytesRef from, BytesRef to, Query dvQuery) {
        return newRelationQuery(field, from, to, RangeFieldQuery.QueryType.WITHIN, dvQuery);
    }

    public static Query newContainsQuery(String field, BytesRef from, BytesRef to, Query dvQuery) {
        return newRelationQuery(field, from, to, RangeFieldQuery.QueryType.CONTAINS, dvQuery);
    }

    /** helper method for creating the desired relational query */
    private static Query newRelationQuery(
        String field,
        final BytesRef min,
        final BytesRef max,
        RangeFieldQuery.QueryType relation,
        Query dvQuery
    ) {
        RangeFieldQuery rangeFieldQuery = new RangeFieldQuery(field, encode(min, max), 1, relation) {
            @Override
            protected String toString(byte[] ranges, int dimension) {
                return Binary16RangeField.toString(ranges, dimension);
            }
        };
        BooleanQuery conjunctionQuery = new BooleanQuery.Builder().add(new BooleanClause(rangeFieldQuery, Occur.MUST))
            .add(new BooleanClause(dvQuery, Occur.MUST)).build();
        return conjunctionQuery;
    }

    /**
     * Returns the String representation for the range at the given dimension
     * @param ranges the encoded ranges, never null
     * @param dimension the dimension of interest (not used for this field)
     * @return The string representation for the range at the provided dimension
     */
    private static String toString(byte[] ranges, int dimension) {
      byte[] min = new byte[BYTES];
      System.arraycopy(ranges, 0, min, 0, BYTES);
      byte[] max = new byte[BYTES];
      System.arraycopy(ranges, BYTES, max, 0, BYTES);
      return "[" + new BytesRef(min) + " : " + new BytesRef(max) + "]";
    }

}
