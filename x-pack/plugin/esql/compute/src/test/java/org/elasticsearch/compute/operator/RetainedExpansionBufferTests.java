/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

/**
 * Checks the actual 5000-row expansion boundary. These passthrough columns model
 * the row marker/original slots; real NameId provenance, PIT identity and parent cancellation remain IT proofs.
 */
public class RetainedExpansionBufferTests extends ComputeTestCase {
    private static final String BODY = "{\"keys\":[\"beta\",\"alpha\",\"beta\"],\"body\":{\"values\":[3,1,3]}}";

    /** Draining both output chunks repeats passthrough values while expanding only the target. */
    public void testActual5000BoundaryPreservesMarkerAndOriginalColumns() {
        BlockFactory factory = blockFactory(ByteSizeValue.ofMb(8));
        try (var operator = new MvExpandOperator(0, 5000)) {
            operator.addInput(input(factory));
            operator.finish();
            try (Page first = operator.getOutput()) {
                assertNotNull(first);
                assertEquals(5000, first.getPositionCount());
                check(first, 0);
                assertTrue(operator.canProduceMoreDataWithoutExtraInput());
            }
            try (Page last = operator.getOutput()) {
                assertNotNull(last);
                assertEquals(1, last.getPositionCount());
                check(last, 5000);
            }
            assertTrue(operator.isFinished());
            assertNull(operator.getOutput());
            assertEquals(2, operator.status().pagesEmitted());
            assertEquals(5001L, operator.status().rowsEmitted());
        }
    }

    /** Driver cleanup closes a partially emitted expansion's raw/expanded backing; no timeout is treated as a release. */
    public void testCloseWithPendingExpansionReleasesBacking() {
        BlockFactory factory = blockFactory(ByteSizeValue.ofMb(8));
        try (var operator = new MvExpandOperator(0, 5000)) {
            operator.addInput(input(factory));
            try (Page first = operator.getOutput()) {
                assertEquals(5000, first.getPositionCount());
                check(first, 0);
                assertTrue("second chunk still buffered before driver-style close", operator.canProduceMoreDataWithoutExtraInput());
            }
        }
        // ComputeTestCase checks every tracked block/array and the request breaker after this method.
    }

    private static Page input(BlockFactory factory) {
        Block[] columns = new Block[4];
        boolean transferred = false;
        try {
            try (var target = factory.newIntBlockBuilder(1)) {
                target.beginPositionEntry();
                for (int value = 0; value < 5001; value++)
                    target.appendInt(value);
                target.endPositionEntry();
                columns[0] = target.build();
            }
            columns[1] = factory.newConstantLongBlockWith(0, 1);
            columns[2] = factory.newConstantBytesRefBlockWith(new BytesRef("A"), 1);
            columns[3] = factory.newConstantBytesRefBlockWith(new BytesRef(BODY), 1);
            Page page = new Page(columns);
            transferred = true;
            return page;
        } finally {
            if (transferred == false) Releasables.close(columns);
        }
    }

    private static void check(Page page, int firstValue) {
        IntBlock expanded = page.getBlock(0);
        LongBlock marker = page.getBlock(1);
        BytesRefBlock id = page.getBlock(2), source = page.getBlock(3);
        BytesRef scratch = new BytesRef();
        BytesRef expectedId = new BytesRef("A"), expectedSource = new BytesRef(BODY);
        for (int row = 0; row < page.getPositionCount(); row++) {
            assertEquals(1, expanded.getValueCount(row));
            assertEquals(firstValue + row, expanded.getInt(expanded.getFirstValueIndex(row)));
            assertEquals(0L, marker.getLong(marker.getFirstValueIndex(row)));
            assertEquals(expectedId, id.getBytesRef(id.getFirstValueIndex(row), scratch));
            assertEquals(expectedSource, source.getBytesRef(source.getFirstValueIndex(row), scratch));
        }
    }
}
