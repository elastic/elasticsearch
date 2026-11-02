/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

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
}
