/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ExternalFieldExtractOperator}: the channel-reshaping logic that drops
 * {@code _rowPosition} from the page, materialises deferred columns via the driver-shared
 * {@link SourceExtractors} registry, and assembles the output page in declared output order.
 */
public class ExternalFieldExtractOperatorTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testReshapeAndExtract() {
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new IntListExtractor(new int[] { 100, 101, 102, 103 }));
            int idB = registry.register(new IntListExtractor(new int[] { 200, 201, 202 }));

            // Build an input page that simulates output of the source: channels are
            // ch0 = sortKey (long), ch1 = _rowPosition (encoded long), ch2 = passThru (int)
            // Five rows surviving TopN, drawn from both extractors:
            // row 0: A[3]
            // row 1: B[1]
            // row 2: A[0]
            // row 3: B[2]
            // row 4: A[2]
            long[] sortKey = { 7L, 8L, 9L, 10L, 11L };
            long[] rowPosition = {
                SourceExtractors.encode(idA, 3),
                SourceExtractors.encode(idB, 1),
                SourceExtractors.encode(idA, 0),
                SourceExtractors.encode(idB, 2),
                SourceExtractors.encode(idA, 2) };
            int[] passThru = { 1, 2, 3, 4, 5 };
            Page input = newPage(sortKey, rowPosition, passThru);

            DriverContext driverContext = mock(DriverContext.class);
            when(driverContext.blockFactory()).thenReturn(blockFactory);

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(/* rowPositionChannel = */ 1,
                /* passThroughChannels = */ List.of(0, 2),
                /* deferredColumnNames = */ List.of("col"),
                /* deferredColumnTypes = */ List.of(DataType.INTEGER),
                registry,
                blockFactory
            );
            op.addInput(input);
            op.finish();
            Page output = op.getOutput();
            assertNull("operator must drain in one shot", op.getOutput());
            assertTrue(op.isFinished());
            try {
                // Output layout: sortKey, passThru, deferred col — _rowPosition stripped.
                assertEquals(3, output.getBlockCount());
                assertEquals(5, output.getPositionCount());

                LongVector outSort = ((LongBlock) output.getBlock(0)).asVector();
                IntVector outPass = ((IntBlock) output.getBlock(1)).asVector();
                IntBlock outDeferred = (IntBlock) output.getBlock(2);

                assertNotNull("sortKey must remain a dense vector", outSort);
                assertNotNull("passThru must remain a dense vector", outPass);
                for (int i = 0; i < 5; i++) {
                    assertEquals(sortKey[i], outSort.getLong(i));
                    assertEquals(passThru[i], outPass.getInt(i));
                }
                // Deferred values must align row-for-row with the surviving (id, pos) refs.
                assertEquals(103, outDeferred.getInt(0));
                assertEquals(201, outDeferred.getInt(1));
                assertEquals(100, outDeferred.getInt(2));
                assertEquals(202, outDeferred.getInt(3));
                assertEquals(102, outDeferred.getInt(4));
            } finally {
                output.releaseBlocks();
                op.close();
            }
        }
    }

    public void testEmptyPageReshape() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1, 2 }));

            Page empty = newPage(new long[0], new long[0], new int[0]);

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory
            );
            op.addInput(empty);
            op.finish();
            Page output = op.getOutput();
            try {
                assertEquals(3, output.getBlockCount());
                assertEquals(0, output.getPositionCount());
                // Deferred slot must be a constant-null placeholder so downstream operators see
                // the right shape even on a zero-row page.
                Block d = output.getBlock(2);
                assertEquals(0, d.getPositionCount());
            } finally {
                output.releaseBlocks();
                op.close();
            }
        }
    }

    public void testFactoryRejectsNullsAndNegatives() {
        SourceExtractors registry = new SourceExtractors();
        try {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(-1, List.of(), List.of(), List.of(), ctx -> registry)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, null, List.of(), List.of(), ctx -> registry)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), null, List.of(), ctx -> registry)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of(), null, ctx -> registry)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of("col"), List.of(), ctx -> registry)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> new ExternalFieldExtractOperator.Factory(0, List.of(), List.of(), List.of(), null)
            );
        } finally {
            registry.close();
        }
    }

    public void testFactoryRejectsNullRegistryLookup() {
        ExternalFieldExtractOperator.Factory factory = new ExternalFieldExtractOperator.Factory(
            0,
            List.of(),
            List.of(),
            List.of(),
            ctx -> null
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        expectThrows(IllegalStateException.class, () -> factory.get(driverContext));
    }

    public void testCloseReleasesPendingPage() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1 }));
            Page page = newPage(new long[] { 1L }, new long[] { SourceExtractors.encode(0, 0) }, new int[] { 9 });

            ExternalFieldExtractOperator op = new ExternalFieldExtractOperator(
                1,
                List.of(0, 2),
                List.of("col"),
                List.of(DataType.INTEGER),
                registry,
                blockFactory
            );
            op.addInput(page);
            // Don't drain; close must release the pending page so we don't leak blocks.
            op.close();
        }
    }

    private Page newPage(long[] sortKey, long[] rowPosition, int[] passThru) {
        assert sortKey.length == rowPosition.length && rowPosition.length == passThru.length;
        int n = sortKey.length;
        Block sortBlock = blockFactory.newLongArrayVector(sortKey, n).asBlock();
        Block rpBlock = blockFactory.newLongArrayVector(rowPosition, n).asBlock();
        Block passBlock = blockFactory.newIntArrayVector(passThru, n).asBlock();
        return new Page(n, sortBlock, rpBlock, passBlock);
    }

    /** Same minimal in-memory column extractor used in {@link SourceExtractorsTests}. */
    private static final class IntListExtractor implements ColumnExtractor {
        private final int[] values;

        IntListExtractor(int[] values) {
            this.values = values;
        }

        @Override
        public long rowCount() {
            return values.length;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
            throws IOException {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    try (IntBlock.Builder builder = factory.newIntBlockBuilder(localPositions.length)) {
                        for (long pos : localPositions) {
                            builder.appendInt(values[Math.toIntExact(pos)]);
                        }
                        result[c] = builder.build();
                    }
                }
                built = true;
                return result;
            } finally {
                if (built == false) org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }

        @Override
        public void close() {}
    }
}
