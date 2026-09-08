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
 * Reproduces the production failure seen when a node reads Parquet under circuit-breaker
 * pressure: a batch in which the pushed filter matches no rows sends every predicate block
 * through {@link PageColumnReader#filterBlock}, whose zero-survivor branch closes the source
 * before allocating its replacement. If that allocation trips the breaker, the caller's array
 * still references the closed block and the cleanup path releases it a second time — surfacing
 * as {@code IllegalStateException: can't release already released object} instead of the
 * {@link CircuitBreakingException} that actually occurred.
 *
 * <p>The shape here mirrors the workload that found it: a string URL column with a
 * {@code LIKE "*google*"} predicate that is not translatable to a Parquet {@code FilterPredicate},
 * so late materialization evaluates it itself, over rows arranged so that whole batches match
 * nothing.
 *
 * <p>Rather than hoping a random breaker trip lands on the vulnerable allocation, the test walks
 * the failure point across every charge the read makes: for each {@code n}, the breaker refuses
 * charge {@code n} and the read must fail cleanly. This covers every allocation site in the batch
 * loop, not just the one that happened to fail in production.
 */
public class ParquetLateMaterializationBreakerTests extends ESTestCase {

    private static final int BATCH_SIZE = 1024;
    private static final int ROWS = 4096;
    /**
     * Rows below this index match nothing, so the first three batches have zero survivors. Above it
     * every other row matches, so the last batch survives partially — which is the shape that
     * reaches the late-materialization fallback, where a second defect lives.
     */
    private static final int FIRST_MATCHING_ROW = 3072;

    /**
     * A counting breaker that refuses one chosen charge. {@code failAtCharge <= 0} never fails,
     * which is how the test counts the charges a clean read makes.
     */
    private static final class FailAtChargeBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final int failAtCharge;
        private int charges;

        FailAtChargeBreaker(int failAtCharge) {
            this.failAtCharge = failAtCharge;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            charges++;
            if (charges == failAtCharge) {
                throw new CircuitBreakingException("breaker refused charge " + charges + " of " + bytes + " bytes", Durability.TRANSIENT);
            }
            used.addAndGet(bytes);
        }

        int charges() {
            return charges;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
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
     * A breaker rejection anywhere in a zero-match batch must surface as a
     * {@link CircuitBreakingException}. Today it surfaces as
     * {@code IllegalStateException: can't release already released object}.
     */
    public void testBreakerTripInZeroMatchBatchSurfacesAsCircuitBreaking() throws IOException {
        byte[] parquetData = urlFileWithZeroMatchBatches();

        FailAtChargeBreaker counting = new FailAtChargeBreaker(-1);
        readToExhaustion(parquetData, counting);
        int totalCharges = counting.charges();
        assertTrue("expected the read to charge the breaker at least once", totalCharges > 0);

        List<String> doubleReleases = new ArrayList<>();
        for (int failAt = 1; failAt <= totalCharges; failAt++) {
            try {
                readToExhaustion(parquetData, new FailAtChargeBreaker(failAt));
            } catch (Exception | AssertionError thrown) {
                if (isBreakerRejection(thrown) == false) {
                    doubleReleases.add("charge " + failAt + ": " + rootCause(thrown));
                }
            }
        }

        assertTrue(
            "a refused breaker charge must surface as CircuitBreakingException, but "
                + doubleReleases.size()
                + " of "
                + totalCharges
                + " charge points released a block twice: "
                + doubleReleases.subList(0, Math.min(5, doubleReleases.size())),
            doubleReleases.isEmpty()
        );
    }

    /**
     * Whatever the outcome, a finished read must leave the breaker at zero. A block that escapes
     * the cleanup path takes its reservation with it, which is silent in logs.
     */
    public void testBreakerTripLeavesNoOutstandingReservation() throws IOException {
        byte[] parquetData = urlFileWithZeroMatchBatches();

        FailAtChargeBreaker counting = new FailAtChargeBreaker(-1);
        readToExhaustion(parquetData, counting);
        int totalCharges = counting.charges();

        List<String> leaks = new ArrayList<>();
        for (int failAt = 1; failAt <= totalCharges; failAt++) {
            FailAtChargeBreaker breaker = new FailAtChargeBreaker(failAt);
            try {
                readToExhaustion(parquetData, breaker);
            } catch (Exception | AssertionError ignored) {
                // Accounting is asserted below regardless of how the read ended.
            }
            if (breaker.getUsed() != 0) {
                leaks.add("charge " + failAt + " leaked " + breaker.getUsed() + " bytes");
            }
        }

        assertTrue(
            "a finished read must return the breaker to zero, but "
                + leaks.size()
                + " charge points leaked: "
                + leaks.subList(0, Math.min(5, leaks.size())),
            leaks.isEmpty()
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
