/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * What row-group pruning is worth on a filtered Parquet scan, and whether reaching it through a multivalue
 * comparison function costs anything over the scalar comparison it is equivalent to.
 *
 * <p>The fixture's predicate column is a plain required primitive, so it holds exactly one value per row and
 * {@code mv_in_range(ts, a, b)} selects the same rows as {@code ts >= a AND ts <= b} from the same data, skipping
 * the same row groups. The ceiling for the multivalue form is therefore the scalar form, and the only thing that
 * can separate them is the per-row cost of the retained filter, which evaluates a different expression in each
 * case. A multivalued column would not measure this: the row-level arm declines there and the comparison stops
 * being like for like.
 *
 * <p>Two mechanisms are in play and {@code clustering} separates them. Sorted, a selective range leaves most row
 * groups unreadable and they are skipped whole. Shuffled, every row group spans the whole range and none can be
 * skipped — but a filtered scan is still far faster than an unfiltered one at {@code wide} projection, because the
 * row-level mask spares the payload columns the decoding of rows that will not survive. At {@code narrow}
 * projection there is no payload to spare and a filter is worth nothing either way.
 *
 * <p>The control is {@code none} across {@code selectivity}: with no filter the parameter is inert, so those two
 * cells run identical work. Their spread is the run's drift floor, and no difference smaller than it means
 * anything.
 */
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParquetFilterPushdownBenchmark {

    private static final int ROWS = 200_000;
    /** Small enough that the fixture holds many row groups, so there is something to skip. */
    private static final int ROW_GROUP_BYTES = 64 * 1024;

    /**
     * The shapes a Kibana panel actually sends. Every one of them is what {@code QueryDslTranslator} produces for
     * the corresponding control, so the benchmark measures the translated vocabulary rather than an invented one:
     * the time picker is a {@code range} on the time field, a filter pill "is" is a {@code match_phrase}, "is one
     * of" is {@code terms}, "is not" is the same pill under {@code must_not}, and "exists" is {@code exists}.
     * A panel always carries the time picker, so every compound shape is that range AND the pill.
     *
     * <p>{@code scalarRange} is the reference the time picker is measured against; {@code none} is the control.
     */
    @Param({ "none", "scalarRange", "mvInRange", "timeAndTerm", "timeAndTerms", "timeAndNotTerm", "timeAndExists", "timeAndNumericRange" })
    public String filterMode;

    @Param({ "1pct", "10pct" })
    public String selectivity;

    @Param({ "clustered", "shuffled" })
    public String clustering;

    /**
     * How many columns beyond the predicate column the query reads. The row-level filter's saving is the decoding it
     * avoids for rows that will not survive, so with one payload column it has almost nothing to do and with eight it
     * has a great deal. A benchmark that does not vary this cannot see the mechanism at all.
     */
    @Param({ "narrow", "wide" })
    public String projection;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private Object pushedFilter;
    private List<String> projectedColumns;

    private static final int PAYLOAD_COLUMNS = 8;

    /** A fixed instant the fixture's timestamps start from, so the column carries plausible epoch millis. */
    private static final long EPOCH_BASE_MILLIS = 1_700_000_000_000L;
    /** One second between rows — a log cadence, and wide enough that no two rows share a millisecond. */
    private static final long TS_STEP_MILLIS = 1_000L;

    /** Distinct values of the {@code svc} keyword column — the cardinality a filter pill typically selects from. */
    private static final String[] SERVICES = { "checkout", "search", "auth", "cart", "billing", "shipping", "reviews", "media" };

    @Setup(Level.Trial)
    public void setup() throws IOException {
        BenchmarkLogging.configure();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        projectedColumns = new ArrayList<>(List.of("id", "ts"));
        if ("wide".equals(projection)) {
            for (int c = 0; c < PAYLOAD_COLUMNS; c++) {
                projectedColumns.add("c" + c);
            }
        }
        byte[] bytes = fixture("clustered".equals(clustering));
        fixtureBytes = bytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(bytes, "memory://filter-bench.parquet");

        // A window anchored at the first timestamp in the fixture, covering the leading share of the range.
        long windowRows = switch (selectivity) {
            case "1pct" -> ROWS / 100L;
            case "10pct" -> ROWS / 10L;
            default -> throw new IllegalArgumentException("unknown selectivity: " + selectivity);
        };
        long from = EPOCH_BASE_MILLIS;
        long to = EPOCH_BASE_MILLIS + windowRows * TS_STEP_MILLIS;
        Expression timeWindow = new MvInRange(Source.EMPTY, ts(), lit(from), lit(to));
        Expression predicate = switch (filterMode) {
            case "none" -> null;
            case "scalarRange" -> new Range(Source.EMPTY, ts(), lit(from), true, lit(to), true, ZoneOffset.UTC);
            case "mvInRange" -> timeWindow;
            // Filter pill "is": match_phrase on a keyword -> mv_contains.
            case "timeAndTerm" -> new And(Source.EMPTY, timeWindow, new MvContains(Source.EMPTY, svc(), keyword(SERVICES[0])));
            // Filter pill "is one of": terms -> mv_intersects over one list-valued literal.
            case "timeAndTerms" -> new And(
                Source.EMPTY,
                timeWindow,
                new MvIntersects(
                    Source.EMPTY,
                    svc(),
                    new Literal(Source.EMPTY, List.of(new BytesRef(SERVICES[0]), new BytesRef(SERVICES[1])), DataType.KEYWORD)
                )
            );
            // Filter pill negated: bool.must_not -> NOT over the same leaf.
            case "timeAndNotTerm" -> new And(
                Source.EMPTY,
                timeWindow,
                new Not(Source.EMPTY, new MvContains(Source.EMPTY, svc(), keyword(SERVICES[0])))
            );
            // Filter pill "exists" -> IS NOT NULL. The one common shape that is not an mv_ form.
            case "timeAndExists" -> new And(Source.EMPTY, timeWindow, new IsNotNull(Source.EMPTY, opt()));
            // A numeric range alongside the time picker, the second range a dashboard commonly carries.
            case "timeAndNumericRange" -> new And(
                Source.EMPTY,
                timeWindow,
                new MvInRange(Source.EMPTY, bytesCol(), longLit(0L), longLit(500L))
            );
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
            .projectedColumns(projectedColumns)
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
        return new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME);
    }

    private static Literal lit(long epochMillis) {
        return new Literal(Source.EMPTY, epochMillis, DataType.DATETIME);
    }

    private static ReferenceAttribute svc() {
        return new ReferenceAttribute(Source.EMPTY, "svc", DataType.KEYWORD);
    }

    private static ReferenceAttribute opt() {
        return new ReferenceAttribute(Source.EMPTY, "opt", DataType.KEYWORD);
    }

    private static ReferenceAttribute bytesCol() {
        return new ReferenceAttribute(Source.EMPTY, "bytes", DataType.LONG);
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static Literal longLit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    /**
     * {@code ts} ascending, or the same values shuffled by a fixed permutation so every row group spans the whole
     * range. The permutation multiplies by {@code 97} modulo {@link #ROWS}; 97 is coprime with {@code 200_000}, so
     * it is a bijection of {@code [0, ROWS)} onto itself and the two fixtures hold exactly the same timestamps —
     * only their order differs. That is what makes {@code clustering} a control: a range filter selects the same
     * number of rows either way, so any difference between the two is pruning and nothing else.
     *
     * <p>{@code ts} is a real {@code TIMESTAMP(MILLIS)} column, one second apart from a fixed instant, so the
     * pushdown reaches {@code buildDatetimePredicate} — the path a filter on a time field actually takes. Written
     * as a bare {@code int64} it would reach {@code buildLongPredicate} instead and measure the wrong arm.
     */
    private static byte[] fixture(boolean clustered) throws IOException {
        StringBuilder schemaText = new StringBuilder(
            "message bench { required int64 id; required int64 ts (TIMESTAMP(MILLIS,true));"
                + " required binary svc (UTF8); optional binary opt (UTF8); required int64 bytes;"
        );
        for (int c = 0; c < PAYLOAD_COLUMNS; c++) {
            schemaText.append(" required binary c").append(c).append(" (UTF8);");
        }
        MessageType schema = MessageTypeParser.parseMessageType(schemaText.append(" }").toString());
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
            for (int i = 0; i < ROWS; i++) {
                long tick = clustered ? i : (i * 97L) % ROWS;
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("ts", EPOCH_BASE_MILLIS + tick * TS_STEP_MILLIS);
                // svc: a low-cardinality keyword, the shape a filter pill matches on. One value in SERVICES is
                // held by 1/SERVICES.length of the rows, so `svc == SERVICES[0]` is an independent selectivity
                // that does not track the time window.
                g.add("svc", SERVICES[(int) (tick % SERVICES.length)]);
                // opt is present on half the rows, so `exists` selects half.
                if (tick % 2 == 0) {
                    g.add("opt", "present-" + tick);
                }
                g.add("bytes", tick % 1000);
                for (int c = 0; c < PAYLOAD_COLUMNS; c++) {
                    g.add("c" + c, "payload-" + c + "-" + i);
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }
}
