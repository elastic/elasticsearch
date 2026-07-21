/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

// THROWAWAY DIFFERENTIAL HARNESS — DO NOT COMMIT.
// Reads every input twice: arm A = pre-PR behaviour (schema from chunk-0/file inference),
// arm B = post-PR behaviour (schema bound to a planner-style projection). Diffs pages cell by cell.

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NdJsonInferenceVsBoundDifferentialHarnessTests extends ESTestCase {

    private BlockFactory blockFactory;
    private ExecutorService executor;

    @Before
    public void setUpHarness() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        executor = Executors.newFixedThreadPool(8);
    }

    @After
    public void tearDownHarness() {
        executor.shutdownNow();
    }

    private static Settings segmentSize64Kb() {
        return Settings.builder().put("esql.datasource.ndjson.segment_size", "64kb").build();
    }

    // ---------- result model ----------

    /** One column's decoded values across all rows; a cell is null, a scalar, or a List for multivalue. */
    private record ColResult(String elementType, List<Object> cells) {}

    private record ReadResult(long rows, List<ColResult> cols, String exception, int warningCount) {
        static ReadResult failure(Throwable t, int warnings) {
            return new ReadResult(-1, List.of(), t.getClass().getSimpleName() + ": " + t.getMessage(), warnings);
        }
    }

    private record Cell(String file, String projection, String policy, String mode) {
        @Override
        public String toString() {
            return file + "/" + projection + "/" + policy + "/" + mode;
        }
    }

    // ---------- file corpus ----------

    private record FileSpec(String name, String content, List<Projection> projections) {}

    private record Projection(String name, List<Attribute> attrs) {}

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Projection proj(String name, Attribute... attrs) {
        return new Projection(name, List.of(attrs));
    }

    private List<FileSpec> corpus() {
        List<FileSpec> specs = new ArrayList<>();

        specs.add(
            new FileSpec(
                "plain",
                "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n{\"a\":3,\"b\":\"z\"}\n",
                List.of(
                    proj("FULL", attr("a", DataType.LONG), attr("b", DataType.KEYWORD)),
                    proj("ABSENT", attr("zzz", DataType.LONG)),
                    proj("EMPTY")
                )
            )
        );

        specs.add(
            new FileSpec(
                "dotted_sib",
                "{\"languages\":5,\"languages.long\":10}\n{\"languages\":6,\"languages.long\":11}\n{\"languages\":7,\"languages.long\":12}\n",
                List.of(
                    proj("FULL", attr("languages", DataType.LONG), attr("languages.long", DataType.LONG)),
                    proj("LEAF", attr("languages.long", DataType.LONG)),
                    proj("PREFIX", attr("languages", DataType.LONG)),
                    proj("EMPTY")
                )
            )
        );

        specs.add(
            new FileSpec(
                "dotted_nosib",
                "{\"languages.long\":10}\n{\"languages.long\":11}\n{\"languages.long\":12}\n",
                List.of(proj("FULL", attr("languages.long", DataType.LONG)), proj("LEAF", attr("languages.long", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "nested",
                "{\"languages\":{\"long\":10}}\n{\"languages\":{\"long\":11}}\n{\"languages\":{\"long\":12}}\n",
                List.of(proj("LEAF", attr("languages.long", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "dotted_partial_prefix",
                "{\"languages\":5,\"languages.long\":1}\n{\"languages.long\":2}\n{\"languages\":6,\"languages.long\":3}\n{\"languages.long\":4}\n",
                List.of(proj("LEAF", attr("languages.long", DataType.LONG)), proj("PREFIX", attr("languages", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "deep_flat",
                "{\"a.b.c\":1}\n{\"a.b.c\":2}\n",
                List.of(proj("LEAF", attr("a.b.c", DataType.LONG)), proj("MIDDLE", attr("a.b", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "deep_mixed",
                "{\"a.b.c\":1}\n{\"a\":{\"b\":{\"c\":2}}}\n{\"a.b\":{\"c\":3}}\n",
                List.of(proj("LEAF", attr("a.b.c", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "types_int_as_long",
                "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n",
                List.of(proj("VLONG", attr("v", DataType.LONG)), proj("VINT", attr("v", DataType.INTEGER)))
            )
        );

        specs.add(
            new FileSpec(
                "types_quoted",
                "{\"v\":\"1\"}\n{\"v\":\"2\"}\n",
                List.of(proj("VLONG", attr("v", DataType.LONG)), proj("VKW", attr("v", DataType.KEYWORD)))
            )
        );

        specs.add(
            new FileSpec(
                "types_mixed_num",
                "{\"v\":1}\n{\"v\":1.5}\n{\"v\":2}\n",
                List.of(proj("VDOUBLE", attr("v", DataType.DOUBLE)), proj("VLONG", attr("v", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec(
                "types_overflow",
                "{\"v\":3000000000}\n{\"v\":1}\n",
                List.of(proj("VINT", attr("v", DataType.INTEGER)), proj("VLONG", attr("v", DataType.LONG)))
            )
        );

        specs.add(
            new FileSpec("nulls_missing", "{\"v\":1}\n{\"v\":null}\n{}\n{\"v\":4}\n", List.of(proj("VLONG", attr("v", DataType.LONG))))
        );

        specs.add(
            new FileSpec("mv_arrays", "{\"m\":[1,2,3]}\n{\"m\":[]}\n{\"m\":[5]}\n", List.of(proj("MLONG", attr("m", DataType.LONG))))
        );

        specs.add(
            new FileSpec(
                "mv_obj_arrays",
                "{\"m\":[{\"x\":1},{\"x\":2}]}\n{\"m\":[{\"x\":3}]}\n",
                List.of(proj("MX", attr("m.x", DataType.LONG)))
            )
        );

        specs.add(new FileSpec("crlf", "{\"a\":1}\r\n{\"a\":2}\r\n", List.of(proj("FULL", attr("a", DataType.LONG)))));

        specs.add(new FileSpec("unterminated", "{\"a\":1}\n{\"a\":2}", List.of(proj("FULL", attr("a", DataType.LONG)))));

        // Big multi-chunk files: segment_size=64kb, ~3+ chunks.
        specs.add(
            new FileSpec(
                "big_prefix_late",
                bigPrefixLate(),
                List.of(
                    proj("LEAF", attr("languages.long", DataType.LONG)),
                    proj("BOTH", attr("languages", DataType.LONG), attr("languages.long", DataType.LONG))
                )
            )
        );
        specs.add(
            new FileSpec(
                "big_prefix_early",
                bigPrefixEarly(),
                List.of(
                    proj("LEAF", attr("languages.long", DataType.LONG)),
                    proj("BOTH", attr("languages", DataType.LONG), attr("languages.long", DataType.LONG))
                )
            )
        );
        specs.add(
            new FileSpec(
                "big_mixed_types_late",
                bigMixedTypesLate(),
                List.of(proj("VDOUBLE", attr("v", DataType.DOUBLE)), proj("VLONG", attr("v", DataType.LONG)))
            )
        );
        specs.add(
            new FileSpec(
                "big_new_col_late",
                bigNewColLate(),
                List.of(proj("W", attr("w", DataType.LONG)), proj("VW", attr("v", DataType.LONG), attr("w", DataType.LONG)))
            )
        );

        return specs;
    }

    /** Chunks 0..n-1 have only the dotted leaf; the prefix sibling appears only after the first 64kb chunk. */
    private static String bigPrefixLate() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 64 * 1024 + 4096) {
            sb.append("{\"languages.long\":").append(i++).append("}\n");
        }
        while (sb.length() < 3 * 64 * 1024) {
            sb.append("{\"languages\":9,\"languages.long\":").append(i++).append("}\n");
        }
        return sb.toString();
    }

    /** The original drift-bug shape: prefix sibling only in the first chunk. */
    private static String bigPrefixEarly() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 64 * 1024 + 4096) {
            sb.append("{\"languages\":9,\"languages.long\":").append(i++).append("}\n");
        }
        while (sb.length() < 3 * 64 * 1024) {
            sb.append("{\"languages.long\":").append(i++).append("}\n");
        }
        return sb.toString();
    }

    /** v is integral in chunk 0, becomes fractional after the first chunk. */
    private static String bigMixedTypesLate() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 64 * 1024 + 4096) {
            sb.append("{\"v\":").append(i++).append("}\n");
        }
        while (sb.length() < 3 * 64 * 1024) {
            sb.append("{\"v\":").append(i++).append(".5}\n");
        }
        return sb.toString();
    }

    /** Column w does not exist in chunk 0 at all; appears after the first chunk. */
    private static String bigNewColLate() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 64 * 1024 + 4096) {
            sb.append("{\"v\":").append(i++).append("}\n");
        }
        while (sb.length() < 3 * 64 * 1024) {
            sb.append("{\"v\":").append(i).append(",\"w\":").append(i++).append("}\n");
        }
        return sb.toString();
    }

    // ---------- read arms ----------

    private List<String> projectedColumnNames(Projection p) {
        return p.attrs().stream().map(Attribute::name).toList();
    }

    /** Arm A, direct whole-file read: reader carries the file-inferred schema; nothing planner-bound. */
    private ReadResult readDirectInference(FileSpec file, Projection p, ErrorPolicy policy) {
        AtomicInteger warnings = new AtomicInteger();
        try {
            byte[] bytes = file.content().getBytes(StandardCharsets.UTF_8);
            StorageObject object = new BytesStorageObject("file:///" + file.name() + ".ndjson", bytes);
            NdJsonFormatReader reader = new NdJsonFormatReader(segmentSize64Kb(), blockFactory);
            var meta = reader.metadata(new BytesStorageObject("file:///" + file.name() + ".ndjson", bytes));
            NdJsonFormatReader bound = meta != null && meta.schema() != null ? reader.withSchema(meta.schema()) : reader;
            FormatReadContext ctx = FormatReadContext.builder()
                .projectedColumns(projectedColumnNames(p))
                .batchSize(1000)
                .errorPolicy(policy)
                .informationalWarningSink(s -> warnings.incrementAndGet())
                .build();
            return collect(bound.read(object, ctx), warnings);
        } catch (Throwable t) {
            return ReadResult.failure(t, warnings.get());
        }
    }

    /** Arm B, direct whole-file read: planner-style projection bound via readSchema. */
    private ReadResult readDirectBound(FileSpec file, Projection p, ErrorPolicy policy) {
        AtomicInteger warnings = new AtomicInteger();
        try {
            byte[] bytes = file.content().getBytes(StandardCharsets.UTF_8);
            StorageObject object = new BytesStorageObject("file:///" + file.name() + ".ndjson", bytes);
            NdJsonFormatReader reader = new NdJsonFormatReader(segmentSize64Kb(), blockFactory);
            FormatReadContext ctx = FormatReadContext.builder()
                .projectedColumns(projectedColumnNames(p))
                .batchSize(1000)
                .errorPolicy(policy)
                .readSchema(p.attrs())
                .informationalWarningSink(s -> warnings.incrementAndGet())
                .build();
            return collect(reader.read(object, ctx), warnings);
        } catch (Throwable t) {
            return ReadResult.failure(t, warnings.get());
        }
    }

    /** Streaming coordinator read; readSchema == null gives arm A (coordinator infers from chunk 0), non-null gives arm B. */
    private ReadResult readCoordinator(FileSpec file, Projection p, ErrorPolicy policy, int parallelism, List<Attribute> readSchema) {
        AtomicInteger warnings = new AtomicInteger();
        try {
            byte[] bytes = file.content().getBytes(StandardCharsets.UTF_8);
            NdJsonFormatReader reader = new NdJsonFormatReader(segmentSize64Kb(), blockFactory);
            CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                reader,
                new ByteArrayInputStream(bytes),
                null,
                projectedColumnNames(p),
                1000,
                parallelism,
                executor,
                policy,
                readSchema,
                0L,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                null,
                -1L,
                StripeColumnScope.PROJECTED,
                new StreamingParallelParsingCoordinator.WarningSinks(s -> warnings.incrementAndGet(), s -> warnings.incrementAndGet())
            );
            return collect(pages, warnings);
        } catch (Throwable t) {
            return ReadResult.failure(t, warnings.get());
        }
    }

    private ReadResult collect(CloseableIterator<Page> pages, AtomicInteger warnings) {
        List<ColResult> cols = new ArrayList<>();
        long rows = 0;
        try (pages) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    if (cols.isEmpty()) {
                        for (int b = 0; b < page.getBlockCount(); b++) {
                            cols.add(new ColResult(page.getBlock(b).elementType().toString(), new ArrayList<>()));
                        }
                    }
                    for (int b = 0; b < page.getBlockCount(); b++) {
                        Block block = page.getBlock(b);
                        List<Object> sink = cols.get(b).cells();
                        for (int pos = 0; pos < page.getPositionCount(); pos++) {
                            sink.add(cellValue(block, pos));
                        }
                    }
                    rows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } catch (Throwable t) {
            return ReadResult.failure(t, warnings.get());
        }
        return new ReadResult(rows, cols, null, warnings.get());
    }

    private static Object cellValue(Block block, int pos) {
        if (block.isNull(pos)) {
            return null;
        }
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        if (count == 1) {
            return scalarValue(block, first);
        }
        List<Object> values = new ArrayList<>(count);
        for (int i = first; i < first + count; i++) {
            values.add(scalarValue(block, i));
        }
        return values;
    }

    private static Object scalarValue(Block block, int index) {
        if (block instanceof IntBlock b) {
            return (long) b.getInt(index);
        }
        if (block instanceof LongBlock b) {
            return b.getLong(index);
        }
        if (block instanceof DoubleBlock b) {
            return b.getDouble(index);
        }
        if (block instanceof BooleanBlock b) {
            return b.getBoolean(index);
        }
        if (block instanceof BytesRefBlock b) {
            return b.getBytesRef(index, new BytesRef()).utf8ToString();
        }
        return "UNSUPPORTED:" + block.getClass().getSimpleName();
    }

    // ---------- diff ----------

    private static String summarize(ReadResult r) {
        if (r.exception() != null) {
            return "EX{" + r.exception() + "}";
        }
        StringBuilder sb = new StringBuilder("rows=").append(r.rows()).append(" warn=").append(r.warningCount());
        for (int c = 0; c < r.cols().size(); c++) {
            ColResult col = r.cols().get(c);
            long nulls = col.cells().stream().filter(v -> v == null).count();
            sb.append(" col").append(c).append("[").append(col.elementType()).append(" nulls=").append(nulls).append("]");
        }
        return sb.toString();
    }

    /** Returns null when equal; otherwise a human-readable divergence description. */
    private static String diff(ReadResult a, ReadResult b) {
        if (a.exception() != null || b.exception() != null) {
            if (a.exception() != null && b.exception() != null) {
                return null; // both failed; classify manually from the summary line
            }
            return "one arm threw: A="
                + (a.exception() == null ? "ok" : a.exception())
                + " B="
                + (b.exception() == null ? "ok" : b.exception());
        }
        StringBuilder sb = new StringBuilder();
        if (a.rows() != b.rows()) {
            sb.append("rowcount A=").append(a.rows()).append(" B=").append(b.rows()).append("; ");
        }
        if (a.cols().size() != b.cols().size()) {
            sb.append("colcount A=").append(a.cols().size()).append(" B=").append(b.cols().size()).append("; ");
            return sb.toString();
        }
        for (int c = 0; c < a.cols().size(); c++) {
            ColResult ca = a.cols().get(c);
            ColResult cb = b.cols().get(c);
            boolean typeDiff = ca.elementType().equals(cb.elementType()) == false;
            int n = Math.min(ca.cells().size(), cb.cells().size());
            int valueDiffs = 0;
            int nullityDiffs = 0;
            List<String> examples = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Object va = ca.cells().get(i);
                Object vb = cb.cells().get(i);
                if ((va == null) != (vb == null)) {
                    nullityDiffs++;
                    if (examples.size() < 3) {
                        examples.add("row" + i + " A=" + va + " B=" + vb);
                    }
                } else if (va != null && semanticEquals(va, vb) == false) {
                    valueDiffs++;
                    if (examples.size() < 3) {
                        examples.add("row" + i + " A=" + va + " B=" + vb);
                    }
                }
            }
            if (typeDiff || valueDiffs > 0 || nullityDiffs > 0) {
                sb.append("col")
                    .append(c)
                    .append("{")
                    .append(typeDiff ? "type A=" + ca.elementType() + " B=" + cb.elementType() + " " : "")
                    .append(valueDiffs > 0 ? "valueDiffs=" + valueDiffs + " " : "")
                    .append(nullityDiffs > 0 ? "nullityDiffs=" + nullityDiffs + " " : "")
                    .append(examples)
                    .append("} ");
            }
        }
        if (a.warningCount() != b.warningCount()) {
            sb.append("warnCount A=").append(a.warningCount()).append(" B=").append(b.warningCount()).append("; ");
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static boolean semanticEquals(Object a, Object b) {
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (semanticEquals(la.get(i), lb.get(i)) == false) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return na.doubleValue() == nb.doubleValue() || na.longValue() == nb.longValue();
        }
        return a.equals(b);
    }

    // ---------- the matrix ----------

    public void testDifferentialMatrix() {
        StringBuilder report = new StringBuilder("\n==== DIFFERENTIAL REPORT ====\n");
        int cells = 0;
        int divergent = 0;
        for (FileSpec file : corpus()) {
            for (Projection p : file.projections()) {
                for (ErrorPolicy policy : List.of(ErrorPolicy.STRICT, ErrorPolicy.PERMISSIVE)) {
                    String policyName = policy == ErrorPolicy.STRICT ? "STRICT" : "NULL_FIELD";
                    for (String mode : List.of("DIRECT", "COORD1", "COORD4")) {
                        ReadResult a;
                        ReadResult b;
                        switch (mode) {
                            case "DIRECT" -> {
                                a = readDirectInference(file, p, policy);
                                b = readDirectBound(file, p, policy);
                            }
                            case "COORD1" -> {
                                a = readCoordinator(file, p, policy, 1, null);
                                b = readCoordinator(file, p, policy, 1, p.attrs());
                            }
                            case "COORD4" -> {
                                a = readCoordinator(file, p, policy, 4, null);
                                b = readCoordinator(file, p, policy, 4, p.attrs());
                            }
                            default -> throw new AssertionError(mode);
                        }
                        cells++;
                        Cell cell = new Cell(file.name(), p.name(), policyName, mode);
                        String d = diff(a, b);
                        if (d != null || a.exception() != null || b.exception() != null) {
                            divergent++;
                            report.append("CELL ").append(cell).append('\n');
                            report.append("  A: ").append(summarize(a)).append('\n');
                            report.append("  B: ").append(summarize(b)).append('\n');
                            if (d != null) {
                                report.append("  DIFF: ").append(d).append('\n');
                            } else {
                                report.append("  DIFF: none (both threw; compare exception text above)\n");
                            }
                        }
                    }
                }
            }
        }
        report.append("==== ").append(cells).append(" cells, ").append(divergent).append(" flagged ====\n");
        fail(report.toString());
    }
}
