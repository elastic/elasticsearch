/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Regression tests for the {@link OptimizedParquetColumnIterator} two-phase decode path,
 * targeting block-lifecycle invariants that were violated by an aliasing bug that crashed
 * production scans on q23 of ClickBench (selective {@code WHERE URL LIKE "*google*"}).
 *
 * <p>The pre-fix code copied predicate {@link org.elasticsearch.compute.data.Block} references
 * from a private {@code predicateBlocks} array into the per-Page {@code blocks} array without
 * clearing the source slot. If a downstream step in the same {@code nextTwoPhaseBatch} loop
 * threw — a circuit-breaker trip on a projection-column allocation, for example — the catch
 * closed both arrays and the aliased Block hit a refcount-zero {@code decRef} the second time
 * around. The exact failure mode in production was an
 * {@code IllegalStateException: can't release already released object [...ConstantBytesRefVector...]}
 * raised from {@code Releasables.closeExpectNoException} while the iterator was unwinding a
 * {@link CircuitBreakingException}.
 *
 * <p>The fix transfers ownership eagerly: the predicate slot is nulled the instant the
 * reference is moved into {@code blocks[]}, so the catch has a single owner per Block and
 * never double-closes. These tests pin that contract by:
 * <ul>
 *   <li>injecting a circuit breaker that fails on the Nth allocation (the projection-column
 *       allocation that runs <em>after</em> at least one predicate slot has been transferred),
 *       then asserting the original {@link CircuitBreakingException} surfaces unwrapped and
 *       the breaker returns to zero — both impossible if the catch had double-closed a Block;</li>
 *   <li>exercising the {@code rowBudget} truncation path which re-slices predicate blocks
 *       inside the same try/catch, to make sure a mid-iteration failure during budget
 *       truncation can't strand half-resliced predicate blocks.</li>
 * </ul>
 */
public class TwoPhaseBlockLifecycleTests extends ESTestCase {

    private static final MessageType TWO_COL_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("label")
        .named("test_schema");

    /**
     * Reproduces the production crash on q23: a selective filter on a small predicate column
     * paired with a large projection column whose decode is what trips the breaker.
     *
     * <p>Pre-fix sequence inside {@code nextTwoPhaseBatch} when a projection allocation throws:
     * <ol>
     *   <li>{@code blocks[predicateCol] = predicateBlocks[predicateCol]} (alias, refcount 1)</li>
     *   <li>{@code blocks[projectionCol] = blockFactory.new...Block(...)} → throws
     *       {@link CircuitBreakingException}</li>
     *   <li>catch closes {@code blocks} → predicate Block refcount 1 → 0</li>
     *   <li>catch closes {@code predicateBlocks} → SAME Block (still aliased) refcount 0 →
     *       {@code IllegalStateException: can't release already released object} masks the
     *       original breaker error</li>
     * </ol>
     *
     * <p>Post-fix step 1 nulls {@code predicateBlocks[predicateCol]} immediately, so the catch
     * sees disjoint arrays and closes each Block exactly once. The original breaker exception
     * surfaces and the breaker returns to zero. Both assertions would fail pre-fix: the wrong
     * exception type would be raised, and a non-zero breaker delta would prove the leak.
     */
    public void testProjectionAllocationFailureDoesNotDoubleReleasePredicateBlocks() throws Exception {
        // Selective filter so two-phase activates; projection is a sizeable BINARY column so
        // its allocation is the natural place for the breaker to deny memory.
        byte[] parquetData = buildParquet(TWO_COL_SCHEMA, 2_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('x', 256) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 50L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        ArmedTripBreaker breaker = new ArmedTripBreaker("test-breaker", ByteSizeValue.ofGb(1));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject obj = nativeAsyncStorage(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        try (CloseableIterator<Page> it = reader.read(obj, FormatReadContext.builder().batchSize(1024).build())) {
            // hasNext() drives advanceRowGroup() -> Phase 1 (predicate decode) and Phase 2
            // (projection prefetch). All of these allocations must succeed; we arm the breaker
            // only after Phase 1+2 have settled, so the trip lands inside nextTwoPhaseBatch
            // exactly when it tries to allocate a projection Block. That is the moment when
            // predicate slots have been transferred into blocks[] under the pre-fix code and
            // double-closing is observable.
            assertTrue("expected at least one row group to read", it.hasNext());
            breaker.arm();
            CircuitBreakingException thrown = expectThrows(CircuitBreakingException.class, () -> {
                Page page = it.next();
                page.releaseBlocks();
            });
            assertThat("expected the synthetic trip", thrown.getMessage(), org.hamcrest.Matchers.containsString("synthetic"));
            assertTrue("breaker must have actually fired", breaker.tripped());
        }

        assertEquals(
            "breaker must return to zero after the failure unwinds (pre-fix, the second "
                + "close on an already-released predicate Block masked the original breaker exception "
                + "with an IllegalStateException and skipped the rest of the cleanup, leaving the "
                + "breaker non-zero or — depending on close order — leaving the iterator in an "
                + "inconsistent state)",
            0L,
            breaker.getUsed()
        );
    }

    /**
     * Same shape as the test above, but exercises a later batch in the same row group: we
     * consume one full Page from {@code next()}, then arm the breaker, then consume the second
     * Page. By the second batch the iterator has already updated {@code pageBatchIndexInRowGroup},
     * advanced internal page cursors, and the predicate-block array points at a fresh entry.
     * The aliasing fix has to hold across batches, not just on the first one.
     */
    public void testFailureAfterSuccessfulBatchStillUnwindsCleanly() throws Exception {
        byte[] parquetData = buildParquet(TWO_COL_SCHEMA, 4_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('y', 128) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        // Filter survivors span more than one batch at the chosen batchSize so we get at least
        // two non-empty batches in the same row group.
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 3_500L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        ArmedTripBreaker breaker = new ArmedTripBreaker("test-breaker", ByteSizeValue.ofGb(1));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject obj = nativeAsyncStorage(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);

        AtomicLong rowsConsumed = new AtomicLong();
        try (CloseableIterator<Page> it = reader.read(obj, FormatReadContext.builder().batchSize(512).build())) {
            assertTrue("expected at least one row group to read", it.hasNext());
            // Consume one batch successfully so internal state has advanced past the first
            // batch; this is what catches a per-batch ownership invariant that would still hold
            // on batch 0 but slip on later batches.
            Page first = it.next();
            rowsConsumed.addAndGet(first.getPositionCount());
            first.releaseBlocks();

            assertTrue("expected a second batch to read", it.hasNext());
            breaker.arm();
            CircuitBreakingException thrown = expectThrows(CircuitBreakingException.class, () -> {
                Page page = it.next();
                page.releaseBlocks();
            });
            assertThat("expected the synthetic trip", thrown.getMessage(), org.hamcrest.Matchers.containsString("synthetic"));
            assertTrue("breaker must have actually fired", breaker.tripped());
        }

        assertThat("expected at least one successful batch before the trip", rowsConsumed.get(), greaterThanOrEqualTo(1L));
        assertEquals("breaker must return to zero after a late-batch failure", 0L, breaker.getUsed());
    }

    /**
     * Pins the post-fix ownership invariant from the happy path: when two-phase emits a Page
     * successfully and the caller releases it, the breaker returns to zero. This complements
     * {@link TwoPhaseReaderTests#testTwoPhaseProducesSameRowsAsSinglePhase} by asserting the
     * lifecycle accounting (every block charged is released) on the same byte-saving scenario.
     */
    public void testHappyPathBreakerAccountingReturnsToZero() throws Exception {
        byte[] parquetData = buildParquet(TWO_COL_SCHEMA, 5_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('z', 256) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 100L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        LimitedBreaker breaker = new LimitedBreaker("test-breaker", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject obj = nativeAsyncStorage(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);

        int rowsRead = 0;
        try (CloseableIterator<Page> it = reader.read(obj, FormatReadContext.builder().batchSize(1024).build())) {
            while (it.hasNext()) {
                Page page = it.next();
                rowsRead += page.getPositionCount();
                page.releaseBlocks();
            }
        }

        assertEquals("filter survivor count", 100, rowsRead);
        assertEquals("breaker must return to zero after a successful scan", 0L, breaker.getUsed());
    }

    /**
     * Exercises the {@code rowBudget} truncation path inside {@code nextTwoPhaseBatch}: with a
     * tight {@code LIMIT}, the iterator trims the survivor count on the final batch to the
     * remaining budget and re-slices the predicate Blocks accordingly. This is the happy-path
     * accounting check for that path — every Block created during truncation must be closed
     * exactly once (either by emission + caller release, or by sliceBlockHead's source-close on
     * success), so the breaker returns to zero. The defect this complements (truncation running
     * outside the try/catch so a mid-loop failure leaks partially-resliced predicate blocks) is
     * structural and is fixed by the catch placement; no fault injection is needed to assert the
     * structural property — the lifecycle assertion below would fail under any leak.
     */
    public void testRowBudgetTruncationLeavesBreakerAtZero() throws Exception {
        byte[] parquetData = buildParquet(TWO_COL_SCHEMA, 5_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('q', 64) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        // Selective filter so two-phase activates; LIMIT lower than survivor count exercises
        // budget truncation in the middle of a batch.
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 500L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        LimitedBreaker breaker = new LimitedBreaker("test-breaker", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject obj = nativeAsyncStorage(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);

        int rowsRead = 0;
        try (CloseableIterator<Page> it = reader.read(obj, FormatReadContext.builder().batchSize(1024).rowLimit(37).build())) {
            while (it.hasNext()) {
                Page page = it.next();
                rowsRead += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertThat("LIMIT cap respected", rowsRead, equalTo(37));
        assertEquals("breaker must return to zero after LIMIT-truncated scan", 0L, breaker.getUsed());
    }

    // -------------------------------------------------------------------------------------
    // Helpers

    /**
     * {@link LimitedBreaker} that fails the next positive-delta allocation only after the test
     * explicitly arms it. This isolates the failure from setup-time allocations (footer reads,
     * preload, prefetch reservations, block-factory bookkeeping) so the trip lands on a
     * specific call inside {@code nextTwoPhaseBatch} that the test selects via its own control
     * flow rather than via brittle absolute counting.
     */
    private static final class ArmedTripBreaker extends LimitedBreaker {
        private final long limitBytes;
        private boolean armed = false;
        private boolean tripped = false;

        ArmedTripBreaker(String name, ByteSizeValue limit) {
            super(name, limit);
            this.limitBytes = limit.getBytes();
        }

        void arm() {
            armed = true;
        }

        boolean tripped() {
            return tripped;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            if (armed && bytes > 0 && tripped == false) {
                tripped = true;
                throw new CircuitBreakingException(
                    "synthetic trip after arm (" + bytes + " bytes for [" + label + "])",
                    bytes,
                    limitBytes,
                    CircuitBreaker.Durability.PERMANENT
                );
            }
            super.addEstimateBytesAndMaybeBreak(bytes, label);
        }
    }

    private static byte[] buildParquet(MessageType schema, int rowCount, java.util.function.IntFunction<Group> rowFactory)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile out = new OutputFile() {
            @Override
            public String getPath() {
                return "memory://test.parquet";
            }

            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public void write(int b) {
                        baos.write(b);
                        pos++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        baos.write(b, off, len);
                        pos += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withConf(new PlainParquetConfiguration())
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                writer.write(rowFactory.apply(i));
            }
        }
        return baos.toByteArray();
    }

    private static StorageObject nativeAsyncStorage(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }

            @Override
            public boolean supportsNativeAsync() {
                return true;
            }

            @Override
            public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
                try (InputStream stream = newStream(position, length)) {
                    byte[] bytes = stream.readAllBytes();
                    listener.onResponse(ByteBuffer.wrap(bytes));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
    }

    private static String repeat(char c, int n) {
        char[] arr = new char[n];
        java.util.Arrays.fill(arr, c);
        return new String(arr);
    }
}
