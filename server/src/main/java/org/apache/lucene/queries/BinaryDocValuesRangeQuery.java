/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.RangeType;

import java.io.IOException;
import java.util.Objects;

public final class BinaryDocValuesRangeQuery extends Query {

    private final String fieldName;
    private final QueryType queryType;
    private final RangeType.LengthType lengthType;
    private final BytesRef from;
    private final BytesRef to;
    private final Object originalFrom;
    private final Object originalTo;

    public BinaryDocValuesRangeQuery(String fieldName, QueryType queryType, RangeType.LengthType lengthType,
                                     BytesRef from, BytesRef to,
                                     Object originalFrom, Object originalTo) {
        this.fieldName = fieldName;
        this.queryType = queryType;
        this.lengthType = lengthType;
        this.from = from;
        this.to = to;
        this.originalFrom = originalFrom;
        this.originalTo = originalTo;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {

                    ByteArrayDataInput in = new ByteArrayDataInput();
                    BytesRef otherFrom = new BytesRef();
                    BytesRef otherTo = new BytesRef();

                    @Override
                    public boolean matches() throws IOException {
                        BytesRef encodedRanges = values.binaryValue();
                        in.reset(encodedRanges.bytes, encodedRanges.offset, encodedRanges.length);
                        int numRanges = in.readVInt();
                        final byte[] bytes = encodedRanges.bytes;
                        otherFrom.bytes = bytes;
                        otherTo.bytes = bytes;
                        int offset = in.getPosition();
                        for (int i = 0; i < numRanges; i++) {
                            int length = lengthType.readLength(bytes, offset);
                            otherFrom.offset = offset;
                            otherFrom.length = length;
                            offset += length;

                            length = lengthType.readLength(bytes, offset);
                            otherTo.offset = offset;
                            otherTo.length = length;
                            offset += length;

                            if (queryType.matches(from, to, otherFrom, otherTo)) {
                                return true;
                            }
                        }
                        assert offset == encodedRanges.offset + encodedRanges.length;
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 4; // at most 4 comparisons
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    @Override
    public String toString(String field) {
        return "BinaryDocValuesRangeQuery(fieldName=" + field + ",from=" + originalFrom + ",to=" + originalTo + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (sameClassAs(o) == false) return false;
        BinaryDocValuesRangeQuery that = (BinaryDocValuesRangeQuery) o;
        return Objects.equals(fieldName, that.fieldName) &&
                queryType == that.queryType &&
                lengthType == that.lengthType &&
                Objects.equals(from, that.from) &&
                Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, queryType, lengthType, from, to);
    }

    public enum QueryType {
        INTERSECTS {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                /*
                 * part of the other range must touch this range
                 * this:    |---------------|
                 * other:               |------|
                 */
                return from.compareTo(otherTo) <= 0 && to.compareTo(otherFrom) >= 0;
            }
        }, WITHIN {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                /*
                 * other range must entirely lie within this range
                 * this:    |---------------|
                 * other:       |------|
                 */
                return from.compareTo(otherFrom) <= 0 && to.compareTo(otherTo) >= 0;
            }
        }, CONTAINS {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                /*
                 * this and other range must overlap
                 * this:       |------|
                 * other:    |---------------|
                 */
                return from.compareTo(otherFrom) >= 0 && to.compareTo(otherTo) <= 0;
            }
        }, CROSSES {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                // does not disjoint AND not within:
                return  (from.compareTo(otherTo) > 0 || to.compareTo(otherFrom) < 0) == false &&
                    (from.compareTo(otherFrom) <= 0 && to.compareTo(otherTo) >= 0) == false;
            }
        };

        abstract boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo);

    }

}
