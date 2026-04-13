/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks parallel parsing throughput with varying parallelism levels.
 * Measures the coordinator overhead and scaling characteristics.
 */
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
public class ParallelParsingBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    private static final List<Attribute> SCHEMA = List.of(
        new FieldAttribute(Source.EMPTY, "line", new EsField("line", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
    );

    @Param({ "1", "2", "4" })
    public int parallelism;

    @Param({ "50000", "200000" })
    public int lineCount;

    private byte[] fileData;
    private ExecutorService executor;

    @Setup(Level.Trial)
    public void setup() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(Locale.ROOT, "%08d", i)).append(",value-").append(i % 1000).append("\n");
        }
        fileData = sb.toString().getBytes(StandardCharsets.UTF_8);
        executor = Executors.newFixedThreadPool(Math.max(parallelism, 1));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Benchmark
    public void parallelParse(Blackhole bh) throws Exception {
        StorageObject obj = new InMemoryStorageObject(fileData);
        BenchLineReader reader = new BenchLineReader();

        try (
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                reader,
                obj,
                List.of("line"),
                1000,
                parallelism,
                executor
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                bh.consume(page.getPositionCount());
                page.releaseBlocks();
            }
        }
    }

    private static class BenchLineReader implements SegmentableFormatReader {

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            byte[] buf = new byte[8192];
            int bytesRead;
            while ((bytesRead = stream.read(buf, 0, buf.length)) > 0) {
                for (int i = 0; i < bytesRead; i++) {
                    consumed++;
                    if (buf[i] == '\n') {
                        return consumed;
                    }
                }
            }
            return -1;
        }

        @Override
        public long minimumSegmentSize() {
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            final byte[] data;
            try (InputStream stream = object.newStream()) {
                data = stream.readAllBytes();
            }
            final int batchSize = context.batchSize();
            final boolean skipFirstLine = context.firstSplit() == false;

            return new CloseableIterator<>() {
                private int pos = 0;
                private Page nextPage = null;

                {
                    // Skip first line if needed (when not first split)
                    if (skipFirstLine) {
                        while (pos < data.length && data[pos] != '\n') {
                            pos++;
                        }
                        if (pos < data.length) {
                            pos++; // skip the \n itself
                        }
                    }
                }

                @Override
                public boolean hasNext() {
                    if (nextPage != null) {
                        return true;
                    }
                    nextPage = readBatch();
                    return nextPage != null;
                }

                @Override
                public Page next() {
                    if (hasNext() == false) {
                        throw new java.util.NoSuchElementException();
                    }
                    Page p = nextPage;
                    nextPage = null;
                    return p;
                }

                private Page readBatch() {
                    if (pos >= data.length) {
                        return null;
                    }
                    int count = 0;
                    // Find up to batchSize lines
                    int[] lineStarts = new int[batchSize];
                    int[] lineLengths = new int[batchSize];

                    while (count < batchSize && pos < data.length) {
                        int lineStart = pos;
                        // Find end of line
                        while (pos < data.length && data[pos] != '\n') {
                            pos++;
                        }
                        int lineLen = pos - lineStart;
                        if (pos < data.length) {
                            pos++; // skip \n
                        }
                        if (lineLen == 0) {
                            continue; // skip empty lines
                        }
                        lineStarts[count] = lineStart;
                        lineLengths[count] = lineLen;
                        count++;
                    }

                    if (count == 0) {
                        return null;
                    }

                    // Build BytesRef views directly into the buffer - zero copy
                    try (var builder = BLOCK_FACTORY.newBytesRefBlockBuilder(count)) {
                        BytesRef ref = new BytesRef();
                        ref.bytes = data;
                        for (int i = 0; i < count; i++) {
                            ref.offset = lineStarts[i];
                            ref.length = lineLengths[i];
                            builder.appendBytesRef(ref);
                        }
                        Block block = builder.build();
                        return new Page(count, block);
                    }
                }

                @Override
                public void close() {
                    // data buffer will be GC'd
                }
            };
        }

        @Override
        public String formatName() {
            return "bench-line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    private static class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            int pos = (int) position;
            if (pos >= data.length) {
                return -1;
            }
            int len = Math.min(target.remaining(), data.length - pos);
            target.put(data, pos, len);
            return len;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("mem://bench");
        }
    }
}
