/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.List;

public class AbstractOTLPBatchBuilderTests extends ESTestCase {

    /** Minimal concrete builder: the abstract class declares no columns of its own. */
    private static final class TestBatchBuilder extends AbstractOTLPBatchBuilder {
        final int a;
        final int b;

        TestBatchBuilder() {
            super(BytesRefRecycler.NON_RECYCLING_INSTANCE);
            a = leafField("a", ROOT);
            b = leafField("b", ROOT);
        }
    }

    /** {@code hasValue} reflects only the current, uncommitted row. */
    public void testHasValueTracksCurrentRow() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            assertFalse(builder.hasValue(builder.a));
            assertFalse(builder.hasValue(builder.b));

            builder.longField(builder.a, 1);
            assertTrue(builder.hasValue(builder.a));
            assertFalse(builder.hasValue(builder.b));
            builder.commitRow();

            assertFalse("a value from a committed row must not count", builder.hasValue(builder.a));
            builder.longField(builder.b, 2);
            assertFalse(builder.hasValue(builder.a));
            assertTrue(builder.hasValue(builder.b));
            builder.commitRow();

            EscfBatch batch = builder.build();
            assertEquals(2, batch.docCount());
        }
    }

    /** Writing a column twice within one row is still rejected for callers that don't check {@code hasValue}. */
    public void testDuplicateWriteWithoutCheckIsRejected() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            builder.longField(builder.a, 1);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.longField(builder.a, 2));
            assertEquals("Duplicate field [a]", e.getMessage());
        }
    }

    /** A fill covers exactly the rows committed between {@code prepareFill} and the fill, leaving the others absent. */
    public void testFillCoversRowsCommittedSincePrepareFill() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            builder.longField(builder.a, 0);
            builder.commitRow();

            int fillFrom = builder.prepareFill();
            for (int i = 1; i <= 3; i++) {
                builder.longField(builder.a, i);
                builder.commitRow();
            }
            builder.fillString(builder.b, "shared", fillFrom);

            builder.longField(builder.a, 4);
            builder.commitRow();

            try (EscfBatch batch = builder.build()) {
                assertEquals(5, batch.docCount());
                ObjectTupleCursor<BytesRef> cursor = batch.column(builder.b).bytesRefCursor(false);
                List<Integer> docs = new ArrayList<>();
                for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                    docs.add(doc);
                    assertEquals("shared", cursor.value().utf8ToString());
                }
                assertEquals(List.of(1, 2, 3), docs);
            }
        }
    }

    /** Filling an empty range writes nothing, so the column is not considered filled. */
    public void testFillOfEmptyRangeIsNoOp() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            int fillFrom = builder.prepareFill();
            builder.fillLong(builder.b, 1, fillFrom);
            assertFalse(builder.isFilled(builder.b, fillFrom));

            builder.longField(builder.a, 1);
            builder.commitRow();
            try (EscfBatch batch = builder.build()) {
                assertEquals(1, batch.docCount());
            }
        }
    }

    /** {@code isFilled} reflects only the current range, and filling a column twice for it is rejected. */
    public void testIsFilledTracksCurrentRange() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            int firstRange = builder.prepareFill();
            builder.longField(builder.a, 1);
            builder.commitRow();
            assertFalse(builder.isFilled(builder.b, firstRange));
            builder.fillLong(builder.b, 1, firstRange);
            assertTrue(builder.isFilled(builder.b, firstRange));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.fillLong(builder.b, 2, firstRange));
            assertEquals("Duplicate field [b]", e.getMessage());

            int secondRange = builder.prepareFill();
            builder.longField(builder.a, 2);
            builder.commitRow();
            assertFalse("a fill of a previous range must not count", builder.isFilled(builder.b, secondRange));
            builder.fillLong(builder.b, 2, secondRange);

            try (EscfBatch batch = builder.build()) {
                LongTupleCursor cursor = batch.column(builder.b).longCursor();
                List<Long> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(cursor.longValue());
                }
                assertEquals(List.of(1L, 2L), values);
            }
        }
    }

    /** A filled column written by a row of the range is a duplicate. */
    public void testFillOfColumnWrittenInRangeIsRejected() {
        try (TestBatchBuilder builder = new TestBatchBuilder()) {
            int fillFrom = builder.prepareFill();
            builder.longField(builder.a, 1);
            builder.commitRow();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.fillLong(builder.a, 2, fillFrom));
            assertEquals("Duplicate field [a]", e.getMessage());
        }
    }
}
