/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

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
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures end-to-end {@link ParquetFormatReader#read} throughput over an in-memory
 * Parquet fixture. Targets the {@code readNextBatch} hot path. Variants cover column
 * projection (all four columns vs a two-column subset) and {@code LIMIT} pushdown
 * (full scan vs early termination at 100 rows). Fixture compression is UNCOMPRESSED
 * to keep codec init out of the measured signal.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParquetReadBenchmark {

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "10000", "100000" })
    public int rowCount;

    @Param({ "all", "projectedSubset" })
    public String projection;

    @Param({ "noLimit", "limit100" })
    public String limitMode;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private List<String> projectedColumns;
    private int rowLimit;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] parquetBytes = generateParquetFixture(rowCount);
        fixtureBytes = parquetBytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(parquetBytes, "memory://bench.parquet");
        projectedColumns = switch (projection) {
            case "all" -> List.of("id", "value", "name", "score");
            case "projectedSubset" -> List.of("id", "name");
            default -> throw new IllegalArgumentException("unknown projection: " + projection);
        };
        rowLimit = switch (limitMode) {
            case "noLimit" -> FormatReader.NO_LIMIT;
            case "limit100" -> 100;
            default -> throw new IllegalArgumentException("unknown limitMode: " + limitMode);
        };
    }

    @Benchmark
    public int readAll(ReadMetrics metrics) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        FormatReadContext ctx = FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(1000).rowLimit(rowLimit).build();
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        metrics.record(totalRows, fixtureBytes);
        return totalRows;
    }

    static void selfTest() {
        for (String projection : Utils.possibleValues(ParquetReadBenchmark.class, "projection")) {
            for (String limitMode : Utils.possibleValues(ParquetReadBenchmark.class, "limitMode")) {
                ParquetReadBenchmark bench = new ParquetReadBenchmark();
                bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
                bench.projection = projection;
                bench.limitMode = limitMode;
                int expected = "limit100".equals(limitMode) ? Math.min(100, bench.rowCount) : bench.rowCount;
                try {
                    bench.setup();
                    int actual = bench.readAll(new ReadMetrics());
                    if (actual != expected) {
                        throw new AssertionError(
                            "ParquetReadBenchmark[" + projection + "/" + limitMode + "] read " + actual + " rows, expected " + expected
                        );
                    }
                } catch (IOException e) {
                    throw new AssertionError("ParquetReadBenchmark[" + projection + "/" + limitMode + "] failed", e);
                }
            }
        }
    }

    private static MessageType schema() {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("bench_schema");
    }

    static byte[] generateParquetFixture(int rowCount) throws IOException {
        MessageType schema = schema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(byteArrayOutputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i);
                g.add("name", "row-" + i);
                g.add("score", i * 1.5);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static OutputFile byteArrayOutputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                        position += len;
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

            @Override
            public String getPath() {
                return "memory://bench.parquet";
            }
        };
    }
}
