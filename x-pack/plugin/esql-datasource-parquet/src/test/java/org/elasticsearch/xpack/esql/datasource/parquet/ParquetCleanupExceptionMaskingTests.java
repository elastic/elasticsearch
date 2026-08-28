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
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * When a read fails and the cleanup that runs on the way out ALSO fails, the exception that
 * actually caused the failure must survive. Today it does not: every cleanup-and-rethrow handler
 * on this path is written as
 *
 * <pre>{@code
 * } catch (CircuitBreakingException e) {
 *     Releasables.closeExpectNoException(blocks);
 *     throw e;
 * }
 * }</pre>
 *
 * and {@code closeExpectNoException} rethrows rather than swallowing, so a failing close propagates
 * out of the handler and {@code throw e} is never reached. The {@link CircuitBreakingException} is
 * discarded by plain Java semantics - not as a cause, not as suppressed - and the query reports an
 * internal server error instead of the 429-classed backpressure that occurred.
 *
 * <p>The failing close is injected through the circuit breaker rather than by mocking a block:
 * {@code Block.close()} releases its reservation via {@code BlockFactory.adjustBreaker(negative)},
 * which calls {@link CircuitBreaker#addWithoutBreaking}. A breaker that throws from there makes a
 * real block's close fail on a real read.
 *
 * <p>The injection is synthetic; the behaviour it demonstrates is not. It is exactly what happened
 * in production, where the failing close was a double release (a separate defect, fixed
 * separately). Fixing whatever makes a close fail removes one trigger; it does not stop the next
 * one from destroying the causal exception the same way.
 */
public class ParquetCleanupExceptionMaskingTests extends ESTestCase {

    private static final int BATCH_SIZE = 1024;
    private static final int ROWS = 4096;
    /**
     * Rows below this index match nothing, so the first three batches have zero survivors. Above it
     * every other row matches, so the last batch survives partially — which is the shape that
     * reaches the late-materialization fallback, where a second defect lives.
     */
    private static final int FIRST_MATCHING_ROW = 3072;

    /**
     * Refuses one chosen charge, and then fails the first release that follows. The two together
     * stage the situation the handler is supposed to survive: a breaker rejection aborts the read,
     * and the cleanup it triggers hits a failing close.
     */
    private static final class FailingReleaseBreaker implements CircuitBreaker {
        static final String BLOCK_FACTORY_LABEL = "<esql_block_factory>";
        static final String REJECTION = "breaker refused the charge";
        static final String RELEASE_FAILURE = "close failed while releasing";

        private final AtomicLong used = new AtomicLong();
        private final int failAtCharge;
        private int charges;
        private boolean armed;
        private boolean releaseFailed;

        FailingReleaseBreaker(int failAtCharge) {
            this.failAtCharge = failAtCharge;
        }

        /**
         * Only block allocations are counted and refused. Charges from the async prefetch path
         * ({@code DirectReadBuffer}) are let through: a rejection there is absorbed by the
         * prefetcher and the read carries on, which would leave the refusal and the later cleanup
         * causally unrelated. Refusing a block allocation puts the rejection inside the batch loop,
         * where the cleanup-and-rethrow handler under test actually runs.
         */
        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (BLOCK_FACTORY_LABEL.equals(label) == false) {
                used.addAndGet(bytes);
                return;
            }
            charges++;
            if (charges == failAtCharge) {
                armed = true;
                throw new CircuitBreakingException(REJECTION, Durability.TRANSIENT);
            }
            used.addAndGet(bytes);
        }

        /**
         * Block.close() releases through here, so this is where a close is made to fail - but only
         * a close that the cleanup path itself is performing. Failing merely "the next release
         * after the rejection" is not enough: several layers absorb a
         * {@link CircuitBreakingException} and carry on, so the next release is usually an ordinary
         * one on a later batch and has nothing to do with the failure being unwound.
         */
        @Override
        public void addWithoutBreaking(long bytes) {
            if (armed && releaseFailed == false && inCleanup()) {
                releaseFailed = true;
                throw new IllegalStateException(RELEASE_FAILURE);
            }
            used.addAndGet(bytes);
        }

        /** True when a cleanup-and-rethrow handler is the one closing us. */
        private static boolean inCleanup() {
            return StackWalker.getInstance()
                .walk(
                    frames -> frames.anyMatch(
                        frame -> frame.getClassName().equals("org.elasticsearch.core.Releasables")
                            && frame.getMethodName().equals("closeExpectNoException")
                    )
                );
        }

        boolean stagedBothFailures() {
            return armed && releaseFailed;
        }

        int charges() {
            return charges;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {}

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return Long.MAX_VALUE;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return CircuitBreaker.REQUEST;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }
    }

    /**
     * A breaker rejection whose cleanup also fails must still surface as a
     * {@link CircuitBreakingException}, with the cleanup failure attached as suppressed. Today the
     * cleanup failure surfaces instead and the rejection is lost outright.
     */
    public void testCleanupFailureMustNotReplaceTheBreakerRejection() throws IOException {
        byte[] parquetData = urlFileWithZeroMatchBatches();

        FailingReleaseBreaker counting = new FailingReleaseBreaker(-1);
        readToExhaustion(parquetData, counting);
        int totalCharges = counting.charges();
        assertTrue("expected the read to charge the breaker at least once", totalCharges > 0);

        List<String> masked = new ArrayList<>();
        int staged = 0;
        for (int failAt = 1; failAt <= totalCharges; failAt++) {
            FailingReleaseBreaker breaker = new FailingReleaseBreaker(failAt);
            Throwable thrown = null;
            try {
                readToExhaustion(parquetData, breaker);
            } catch (Exception | AssertionError caught) {
                thrown = caught;
            }
            if (breaker.stagedBothFailures() == false) {
                continue; // this charge point did not produce a cleanup release; nothing to judge
            }
            staged++;
            if (thrown == null) {
                masked.add("charge " + failAt + ": read completed despite a refused charge");
            } else if (isBreakerRejection(thrown) == false) {
                masked.add("charge " + failAt + ": surfaced " + rootCause(thrown) + " instead of the rejection");
            }
        }

        assertTrue("no charge point staged both a rejection and a failing cleanup release", staged > 0);
        assertTrue(
            "a rejection whose cleanup also fails must still surface as CircuitBreakingException, but "
                + masked.size()
                + " of "
                + staged
                + " such points lost it: "
                + masked.subList(0, Math.min(5, masked.size())),
            masked.isEmpty()
        );
    }

    /**
     * True when the read ended the way a refused charge should end it. With assertions enabled a
     * double release surfaces as an {@link AssertionError} from
     * {@code Releasables.closeExpectNoException} rather than the underlying
     * {@link IllegalStateException}, so both have to be told apart from a genuine rejection.
     */
    private static boolean isBreakerRejection(Throwable thrown) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t instanceof CircuitBreakingException) {
                return true;
            }
        }
        return false;
    }

    private static String rootCause(Throwable thrown) {
        Throwable t = thrown;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    /**
     * Reads the whole file, releasing every page. Propagates whatever the reader throws.
     */
    private void readToExhaustion(byte[] parquetData, CircuitBreaker breaker) throws IOException {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        ReferenceAttribute url = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression like = new WildcardLike(Source.EMPTY, url, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like));

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        try (CloseableIterator<Page> pages = reader.read(storageObject(parquetData), FormatReadContext.of(null, BATCH_SIZE))) {
            while (pages.hasNext()) {
                pages.next().releaseBlocks();
            }
        }
    }

    /**
     * A URL column shaped like the workload that found this: the first {@link #FIRST_MATCHING_ROW}
     * rows match nothing, so whole batches are filtered away, and a second column gives late
     * materialization something to defer.
     */
    private byte[] urlFileWithZeroMatchBatches() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("search_phrase")
            .required(INT64)
            .named("counter")
            .named("late_mat_breaker_test");

        return writeParquet(schema, factory -> {
            List<Group> groups = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                boolean matches = i >= FIRST_MATCHING_ROW && i % 2 == 0;
                String url = matches ? "https://www.google.com/search?q=" + i : "https://example.org/page?id=" + i;
                groups.add(factory.newGroup().append("url", url).append("search_phrase", "phrase_" + i).append("counter", (long) i));
            }
            return groups;
        });
    }

    /**
     * The same row shape as {@link #urlFileWithZeroMatchBatches} plus a LIST&lt;INT64&gt; column
     * ({@code tags}, standard 3-level encoding). Its presence disables two-phase I/O and routes
     * its decode through {@code ColumnReader}, which is what exposes the Phase-3 fallback arm in
     * the partially-matching final batch.
     */
    private byte[] urlFileWithListColumnAndZeroMatchBatches() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .optionalGroup()
            .as(LogicalTypeAnnotation.listType())
            .repeatedGroup()
            .optional(INT64)
            .named("element")
            .named("list")
            .named("tags")
            .named("late_mat_list_breaker_test");

        return writeParquet(schema, factory -> {
            List<Group> groups = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                boolean matches = i >= FIRST_MATCHING_ROW && i % 2 == 0;
                String url = matches ? "https://www.google.com/search?q=" + i : "https://example.org/page?id=" + i;
                Group group = factory.newGroup().append("url", url);
                Group tags = group.addGroup("tags");
                tags.addGroup("list").add("element", (long) i);
                tags.addGroup("list").add("element", (long) i * 2);
                groups.add(group);
            }
            return groups;
        });
    }

    @FunctionalInterface
    private interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    private byte[] writeParquet(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(bytes))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(10 * 1024 * 1024)
                .withPageSize(64)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Group group : groupCreator.create(groupFactory)) {
                writer.write(group);
            }
        }
        return bytes.toByteArray();
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://late_mat_breaker_test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream bytes) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(bytes);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionOutputStream(bytes);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://late_mat_breaker_test.parquet";
            }
        };
    }

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream bytes) {
        return new PositionOutputStream() {
            @Override
            public long getPos() {
                return bytes.size();
            }

            @Override
            public void write(int b) {
                bytes.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                bytes.write(b, off, len);
            }
        };
    }
}
