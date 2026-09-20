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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
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
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * What row-group pruning is worth on a filtered Parquet scan, and whether reaching it through a multivalue
 * comparison function costs anything over the scalar comparison it is equivalent to.
 *
 * <p>A predicate is only ever pushed over a column that cannot hold more than one value per row — the builders
 * decline any repeated or group column — so {@code mv_in_range(ts, a, b)} and {@code ts >= a AND ts <= b} select
 * the same rows from the same data and skip the same row groups. The ceiling for the multivalue form is therefore
 * the scalar form, and the only thing that can separate them is the per-row cost of the retained filter, which
 * evaluates a different expression in each case.
 *
 * <p>{@code clustering} is the control. Sorted, a selective range leaves most row groups unreadable and pruning
 * pays. Shuffled, every row group spans the whole range, nothing can be skipped, and all three modes must land
 * together — if they do not, the benchmark is measuring something other than pruning.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParquetFilterPushdownBenchmark {

    private static final int ROWS = 200_000;
    /** Small enough that the fixture holds many row groups, so there is something to skip. */
    private static final int ROW_GROUP_BYTES = 64 * 1024;

    @Param({ "none", "scalarRange", "mvInRange" })
    public String filterMode;

    @Param({ "1pct", "10pct" })
    public String selectivity;

    @Param({ "clustered", "shuffled" })
    public String clustering;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private Object pushedFilter;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        BenchmarkLogging.configure();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] bytes = fixture("clustered".equals(clustering));
        fixtureBytes = bytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(bytes, "memory://filter-bench.parquet");

        long upper = switch (selectivity) {
            case "1pct" -> ROWS / 100L;
            case "10pct" -> ROWS / 10L;
            default -> throw new IllegalArgumentException("unknown selectivity: " + selectivity);
        };
        Expression predicate = switch (filterMode) {
            case "none" -> null;
            case "scalarRange" -> new Range(Source.EMPTY, ts(), lit(0L), true, lit(upper), true, ZoneOffset.UTC);
            case "mvInRange" -> new MvInRange(Source.EMPTY, ts(), lit(0L), lit(upper));
            default -> throw new IllegalArgumentException("unknown filterMode: " + filterMode);
        };
        // The planner's own path, so the benchmark cannot push something the engine would not.
        pushedFilter = predicate == null ? null : new ParquetFilterPushdownSupport().pushFilters(List.of(predicate)).pushedFilter();
        if (predicate != null && pushedFilter == null) {
            throw new IllegalStateException("[" + filterMode + "] did not push; the benchmark would measure nothing");
        }
    }

    @Benchmark
    public int filteredScan(ReadMetrics metrics) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory).withPushedFilter(pushedFilter);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("id", "ts"))
            .batchSize(1000)
            .rowLimit(FormatReader.NO_LIMIT)
            .build();
        int rows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        metrics.record(rows, fixtureBytes);
        return rows;
    }

    private static ReferenceAttribute ts() {
        return new ReferenceAttribute(Source.EMPTY, "ts", DataType.LONG);
    }

    private static Literal lit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    /**
     * {@code ts} ascending, or the same values shuffled by a fixed permutation so every row group spans the whole
     * range. The permutation is a multiplication modulo a prime above {@link #ROWS}, so it is a bijection and the
     * two fixtures hold exactly the same values — only their order differs.
     */
    private static byte[] fixture(boolean clustered) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("ts")
            .named("bench");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        OutputFile outputFile = ParquetReadBenchmark.byteArrayOutputFile(out);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(ROW_GROUP_BYTES)
                .build()
        ) {
            final long prime = 200_003L;
            for (int i = 0; i < ROWS; i++) {
                long ts = clustered ? i : (i * 97L) % prime;
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("ts", ts);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }
}
