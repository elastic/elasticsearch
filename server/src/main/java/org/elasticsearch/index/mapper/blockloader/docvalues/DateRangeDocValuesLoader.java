/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BinaryRangeUtil;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

public class DateRangeDocValuesLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public DateRangeDocValuesLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longRangeBuilder(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        TrackingBinaryDocValues dv = TrackingBinaryDocValues.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        return new DateRangeDocValuesReader(dv);
    }

    private class DateRangeDocValuesReader extends BlockDocValuesReader {
        private final TrackingBinaryDocValues docValues;

        DateRangeDocValuesReader(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        private int docId = -1;

        @Override
        protected int docId() {
            return docId;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.DateRangeDocValuesReader";
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (BlockLoader.LongRangeBuilder builder = factory.longRangeBuilder(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    lastDoc = doc;
                    this.docId = doc;
                    if (false == docValues.docValues().advanceExact(doc)) {
                        builder.appendNull();
                    } else {
                        BytesRef ref = docValues.docValues().binaryValue();
                        var ranges = BinaryRangeUtil.decodeLongRanges(ref);
                        if (ranges.isEmpty()) {
                            builder.appendNull();
                        } else if (ranges.size() == 1) {
                            var range = ranges.get(0);
                            // While the index stores ranges with fully-inclusive bounds [from, to], in ESQL we always
                            // represent and use date ranges as half-open [from, to), so we add 1 to the upper bound here.
                            // If the stored upper bound is Long.MAX_VALUE we cannot add 1 without overflow, so we leave
                            // it as Long.MAX_VALUE — effectively treating it as an open/unbounded upper end.
                            builder.from().appendLong((long) range.getFrom());
                            builder.to().appendLong(inclusiveToExclusive((long) range.getTo()));
                        } else {
                            builder.from().beginPositionEntry();
                            builder.to().beginPositionEntry();
                            for (var range : ranges) {
                                builder.from().appendLong((long) range.getFrom());
                                builder.to().appendLong(inclusiveToExclusive((long) range.getTo()));
                            }
                            builder.from().endPositionEntry();
                            builder.to().endPositionEntry();
                        }
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            docValues.close();
        }
    }

    /**
     * Converts an inclusive upper bound (as stored in Lucene) to the exclusive upper bound used
     * by ES|QL. Adding 1 is safe for all values except {@code Long.MAX_VALUE}, where it would
     * overflow to {@code Long.MIN_VALUE} and corrupt every downstream operation. Instead we
     * return {@code Long.MAX_VALUE} unchanged, treating it as an open/unbounded sentinel.
     *
     * <p>Semantic impact of the sentinel: the single millisecond {@code Long.MAX_VALUE} (~year
     * 292&thinsp;271&thinsp;023 CE) is excluded from the ES|QL representation even though it is
     * included in the stored range. This produces:
     * <ul>
     *   <li><b>False negatives</b> in {@code RANGE_WITHIN} and {@code RANGE_INTERSECTS} only when
     *       the tested point or range starts exactly at {@code Long.MAX_VALUE} — unreachable with
     *       real dates.</li>
     *   <li><b>False positive equality</b>: a range stored with {@code to = Long.MAX_VALUE - 1}
     *       (inclusive) and one stored with {@code to = Long.MAX_VALUE} (inclusive) both map to
     *       {@code to = Long.MAX_VALUE} in ES|QL and therefore compare as equal. Same unreachable
     *       boundary.</li>
     *   <li><b>No impact</b> on range-in-range {@code RANGE_WITHIN}: {@code a.to() <=
     *       Long.MAX_VALUE} is always {@code true}, which correctly treats the sentinel as
     *       "unbounded above".</li>
     * </ul>
     */
    static long inclusiveToExclusive(long inclusiveTo) {
        return inclusiveTo == Long.MAX_VALUE ? Long.MAX_VALUE : inclusiveTo + 1;
    }
}
