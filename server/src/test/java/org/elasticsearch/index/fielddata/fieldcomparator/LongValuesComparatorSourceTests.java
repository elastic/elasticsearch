/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.fielddata.DenseLongValues;
import org.elasticsearch.test.ESTestCase;

public class LongValuesComparatorSourceTests extends ESTestCase {

    /**
     * The {@link NumericDocValues} produced by {@link LongValuesComparatorSource#wrap} is used as a competitive iterator
     * during sorting. When such a sort runs under a conjunction query, Lucene 10.5's {@code DenseConjunctionBulkScorer}
     * calls {@link DocIdSetIterator#cost()} on every conjunction clause while scoring a window. The wrapper used to throw
     * {@link UnsupportedOperationException} from {@code cost()}, which surfaced as a query failure after the Lucene 10.5
     * upgrade (and, downstream, as an ML data-frame-analytics inference failure since {@code TestDocsIterator} search-afters
     * the destination index sorted by {@code ml__incremental_id}). The wrapper iterates densely over {@code [0, maxDoc)},
     * so its cost must be {@code maxDoc}, matching the sibling {@link org.elasticsearch.index.fielddata.DenseDoubleValues}.
     */
    public void testWrapReportsDenseCost() throws Exception {
        long[] values = new long[] { 5, -3, 42, 0, 17 };
        int maxDoc = values.length;
        NumericDocValues docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);

        // Must not throw and must report the dense cost, exactly as Lucene 10.5's conjunction bulk scorer expects.
        assertEquals(maxDoc, docValues.cost());
    }

    public void testWrapIteratesDenselyOverAllDocs() throws Exception {
        long[] values = new long[] { 5, -3, 42, 0, 17 };
        int maxDoc = values.length;

        // Iterating with nextDoc() must visit every document in order and expose the right value for each.
        NumericDocValues docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);
        assertEquals(-1, docValues.docID());
        for (int doc = 0; doc < maxDoc; doc++) {
            assertEquals(doc, docValues.nextDoc());
            assertEquals(doc, docValues.docID());
            assertEquals(values[doc], docValues.longValue());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.nextDoc());

        // advance() past the last document terminates the iterator.
        docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);
        assertEquals(2, docValues.advance(2));
        assertEquals(values[2], docValues.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.advance(maxDoc));
    }

    private static DenseLongValues denseLongValues(long[] values) {
        return new DenseLongValues() {
            private long current;

            @Override
            protected void doAdvanceExact(int doc) {
                current = values[doc];
            }

            @Override
            public long longValue() {
                return current;
            }
        };
    }
}
