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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.planner.Layout;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
 * groups unreadable and they are skipped whole. Shuffled, every row group and every page spans the whole range, so
 * nothing can be skipped by statistics, and the only thing left that can spare the payload columns is the row-level
 * mask. At {@code narrow} projection there is no payload to spare.
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
    private ExpressionEvaluator retainedFilter;
    private DriverContext driverContext;

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
        // The predicate columns are in the page because the retained filter has to read them: predicateColumnNames
        // drives their materialization in a real query too, so leaving them out would measure a page the engine
        // never produces.
        projectedColumns = new ArrayList<>(List.of("id", "ts", "svc", "opt", "bytes"));
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
            // The scalar reference, written the way a user writes it: WHERE ts >= a AND ts <= b. A Range node is
            // what the optimizer folds that into, but EvalMapper has no Range arm, so the retained filter could
            // not evaluate it and the reference would be the one shape paying no downstream cost.
            case "scalarRange" -> new And(
                Source.EMPTY,
                new GreaterThanOrEqual(Source.EMPTY, ts(), lit(from), null),
                new LessThanOrEqual(Source.EMPTY, ts(), lit(to), null)
            );
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

        // Every mv_ form pushes as RECHECK, so the exact predicate is retained above the source and evaluated on
        // every row the reader emits. Without this step a row the reader declined to drop is free, which scores
        // deferred work as saved and makes any row-level filter look like pure cost.
        driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        if (predicate == null) {
            retainedFilter = null;
        } else {
            Layout.Builder layout = new Layout.Builder();
            for (String name : projectedColumns) {
                layout.append(List.of(attributeFor(name)));
            }
            retainedFilter = EvalMapper.toEvaluator(FoldContext.small(), predicate, layout.build()).get(driverContext);
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
        int survivors = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                rows += page.getPositionCount();
                survivors += retainedFilter == null ? page.getPositionCount() : countSurvivors(page);
                page.releaseBlocks();
            }
        }
        metrics.record(rows, fixtureBytes);
        return survivors;
    }

    /** What the retained FilterExec does above the source: evaluate the exact predicate on every emitted row. */
    private int countSurvivors(Page page) {
        try (Block result = retainedFilter.eval(page)) {
            BooleanBlock kept = (BooleanBlock) result;
            int n = 0;
            for (int i = 0; i < kept.getPositionCount(); i++) {
                if (kept.isNull(i) == false && kept.getValueCount(i) == 1 && kept.getBoolean(kept.getFirstValueIndex(i))) {
                    n++;
                }
            }
            return n;
        }
    }

    // One instance per column, shared between the predicate and the layout. A ReferenceAttribute carries a fresh
    // NameId per construction, and Layout resolves by NameId, so a second instance of the same column is a
    // different attribute to the evaluator and resolves to nothing.
    private static final ReferenceAttribute ID = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
    private static final ReferenceAttribute TS = new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME);
    private static final ReferenceAttribute SVC = new ReferenceAttribute(Source.EMPTY, "svc", DataType.KEYWORD);
    private static final ReferenceAttribute OPT = new ReferenceAttribute(Source.EMPTY, "opt", DataType.KEYWORD);
    private static final ReferenceAttribute BYTES = new ReferenceAttribute(Source.EMPTY, "bytes", DataType.LONG);

    private static ReferenceAttribute attributeFor(String name) {
        return switch (name) {
            case "id" -> ID;
            case "ts" -> TS;
            case "svc" -> SVC;
            case "opt" -> OPT;
            case "bytes" -> BYTES;
            default -> new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
        };
    }

    private static ReferenceAttribute ts() {
        return TS;
    }

    private static Literal lit(long epochMillis) {
        return new Literal(Source.EMPTY, epochMillis, DataType.DATETIME);
    }

    private static ReferenceAttribute svc() {
        return SVC;
    }

    private static ReferenceAttribute opt() {
        return OPT;
    }

    private static ReferenceAttribute bytesCol() {
        return BYTES;
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static Literal longLit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    /**
     * The fixture. Every column is derived from one tick per row, so the two layouts hold exactly the same rows and
     * differ only in their order — which is what makes {@code clustering} a control.
     *
     * <p>{@code ts} is a real {@code TIMESTAMP(MILLIS)} column, one second apart from a fixed instant, so the
     * pushdown reaches {@code buildDatetimePredicate} — the path a filter on a time field actually takes.
     *
     * <p>{@code svc}, {@code opt} and {@code bytes} are derived from the tick modulo a small number, so in both
     * layouts every row group and every page holds every one of their values. Statistics can therefore prune on the
     * time range alone; a filter on those columns can only be answered row by row, which is the case the row-level
     * mask exists for.
     */
    private static int[] ticks(boolean clustered) {
        int[] ticks = new int[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ticks[i] = i;
        }
        if (clustered == false) {
            // A seeded Fisher-Yates shuffle. An earlier version multiplied by a constant modulo ROWS, which is a
            // bijection but keeps neighbouring rows a fixed step apart, so every page still held a narrow run of
            // timestamps and the page index pruned most of them. That hid the row-level mask behind page pruning.
            // Here every page spans the whole range, so neither row groups nor pages can be skipped.
            Random random = new Random(0x5EEDL);
            for (int i = ROWS - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                int swap = ticks[i];
                ticks[i] = ticks[j];
                ticks[j] = swap;
            }
        }
        return ticks;
    }

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
            int[] ticks = ticks(clustered);
            for (int i = 0; i < ROWS; i++) {
                long tick = ticks[i];
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
