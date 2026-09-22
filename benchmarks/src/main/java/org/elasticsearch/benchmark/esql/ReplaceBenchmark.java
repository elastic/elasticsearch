/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@code REPLACE(str, regex, newStr)} through the full eval pipeline. Expression mirrors
 * ClickBench's referer-domain query. Input is a plain (non-ordinal) {@link BytesRefVector} so every
 * scenario hits the per-row path (dictionary/ordinal fast paths are out of scope here).
 * <p>
 * {@link #replace} goes through the real {@code Replace#toEvaluator} production wiring: for this
 * expression's exact regex/newStr shape, that now resolves to {@code ReplaceCaptureUntilDelimiterEvaluator}
 * (the idiom-detected byte-scan fast path), not the older {@code ReplaceConstantOrdinalEvaluator}
 * page-scoped memoization cache. {@link #replaceByteScan} is a hand-copied duplicate of the same
 * byte-scan logic, kept as an independent cross-check that the generated evaluator's overhead (block/
 * vector dispatch, warnings, etc.) doesn't meaningfully regress the bare algorithm -- the two should
 * track closely. {@link #setup} asserts which evaluator {@link #replace} actually got, so a future
 * change that silently disables the idiom match is caught here rather than by a quiet perf regression.
 * <p>
 * Scenarios:
 * <ul>
 *   <li><b>clickbenchReal</b> — real {@code hits.Referer} values, original order (~24% distinct, ~53%
 *       adjacent-duplicate). The target workload.</li>
 *   <li><b>clickbenchRealShuffled</b> — same values, row order shuffled (no adjacency). Isolates the
 *       hash-map tier and stress-tests the "give up if mostly distinct" guard.</li>
 *   <li><b>allDistinct</b> — synthetic, no repeats. Regression bound.</li>
 *   <li><b>singleValue</b> — synthetic, one value for the whole page. Speedup bound.</li>
 * </ul>
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ReplaceBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    // ~ row count of a 256KB ES|QL page for an ~80-100 byte keyword column (ClickBench's hits.Referer).
    private static final int BLOCK_LENGTH = 2048;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    @Param({ "clickbenchReal", "clickbenchRealShuffled", "allDistinct", "singleValue" })
    public String scenario;

    private ExpressionEvaluator evaluator;
    // Non-regex rewrite, chained as separate EVAL-like steps (each references the *previous* step's
    // output column by channel, like real sequential EVALs would) -- not one giant nested expression,
    // which would silently re-evaluate shared sub-expressions once per occurrence.
    private ExpressionEvaluator schemeStrippedEvaluator;
    private ExpressionEvaluator wwwStrippedEvaluator;
    private ExpressionEvaluator slashPosEvaluator;
    private ExpressionEvaluator kEvaluator;
    private Page page;
    private BytesRefVector byteScanVector;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);
        String[] values = switch (scenario) {
            case "clickbenchReal" -> tile(loadRefererSample(), BLOCK_LENGTH);
            case "clickbenchRealShuffled" -> shuffle(tile(loadRefererSample(), BLOCK_LENGTH), random);
            case "allDistinct" -> distinctValues(BLOCK_LENGTH);
            case "singleValue" -> singleValue(BLOCK_LENGTH);
            default -> throw new UnsupportedOperationException("unknown scenario: " + scenario);
        };

        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "referer",
            new EsField("referer", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        Layout layout = layoutBuilder.build();

        // REPLACE(Referer, "^https?://(?:www\.)?([^/]+)/.*$", "$1")
        Expression replaceExpr = new Replace(
            Source.EMPTY,
            field,
            new Literal(Source.EMPTY, new BytesRef("^https?://(?:www\\.)?([^/]+)/.*$"), DataType.KEYWORD),
            new Literal(Source.EMPTY, new BytesRef("$1"), DataType.KEYWORD)
        );
        evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, replaceExpr, layout).get(driverContext);
        // Guard against a future change silently disabling the idiom match and falling back to the
        // (much slower) old cached-regex evaluator without anyone noticing in the numbers.
        if (evaluator.toString().contains("ReplaceCaptureUntilDelimiterEvaluator") == false) {
            throw new IllegalStateException(
                "expected the idiom-detected fast path, got: " + evaluator + " -- update this benchmark's javadoc if intentional"
            );
        }

        // Non-regex rewrite of the same extraction, as *sequential* EVAL-like steps -- each references
        // the prior step's materialized column by channel (real EVAL chains never re-run a shared
        // sub-expression; a naive single nested-expression tree would, and massively inflates the cost):
        // schemeStripped = CASE(STARTS_WITH(Referer,"https://"), SUBSTRING(Referer,9),
        // STARTS_WITH(Referer,"http://"), SUBSTRING(Referer,8), NULL)
        // wwwStripped = CASE(STARTS_WITH(schemeStripped,"www."), SUBSTRING(schemeStripped,5), schemeStripped)
        // slashPos = LOCATE(wwwStripped, "/")
        // k = CASE(slashPos > 0, SUBSTRING(wwwStripped, 1, slashPos - 1), Referer)
        Expression schemeStrippedExpr = new Case(
            Source.EMPTY,
            new StartsWith(Source.EMPTY, field, keyword("https://")),
            List.of(
                new Substring(Source.EMPTY, field, intLit(9), null),
                new StartsWith(Source.EMPTY, field, keyword("http://")),
                new Substring(Source.EMPTY, field, intLit(8), null),
                new Literal(Source.EMPTY, null, DataType.NULL)
            )
        );
        schemeStrippedEvaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, schemeStrippedExpr, layout).get(driverContext);

        FieldAttribute schemeStrippedField = new FieldAttribute(
            Source.EMPTY,
            "schemeStripped",
            new EsField("schemeStripped", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Layout layout1 = new Layout.Builder().append(List.of(field, schemeStrippedField)).build();
        Expression wwwStrippedExpr = new Case(
            Source.EMPTY,
            new StartsWith(Source.EMPTY, schemeStrippedField, keyword("www.")),
            List.of(new Substring(Source.EMPTY, schemeStrippedField, intLit(5), null), schemeStrippedField)
        );
        wwwStrippedEvaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, wwwStrippedExpr, layout1).get(driverContext);

        FieldAttribute wwwStrippedField = new FieldAttribute(
            Source.EMPTY,
            "wwwStripped",
            new EsField("wwwStripped", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Layout layout2 = new Layout.Builder().append(List.of(field, schemeStrippedField, wwwStrippedField)).build();
        Expression slashPosExpr = new Locate(Source.EMPTY, wwwStrippedField, keyword("/"), null);
        slashPosEvaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, slashPosExpr, layout2).get(driverContext);

        FieldAttribute slashPosField = new FieldAttribute(
            Source.EMPTY,
            "slashPos",
            new EsField("slashPos", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Layout layout3 = new Layout.Builder().append(List.of(field, schemeStrippedField, wwwStrippedField, slashPosField)).build();
        Expression kExpr = new Case(
            Source.EMPTY,
            new GreaterThan(Source.EMPTY, slashPosField, intLit(0), ZoneOffset.UTC),
            List.of(
                new Substring(
                    Source.EMPTY,
                    wwwStrippedField,
                    intLit(1),
                    new Sub(Source.EMPTY, slashPosField, intLit(1), EsqlTestUtils.TEST_CFG)
                ),
                field
            )
        );
        kEvaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, kExpr, layout3).get(driverContext);

        BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (String value : values) {
            builder.appendBytesRef(new BytesRef(value));
        }
        var vector = builder.build();
        var block = vector.asBlock();
        if (block.asOrdinals() != null) {
            throw new IllegalStateException("benchmark setup must produce a plain, non-ordinal block");
        }
        page = new Page(block);
        byteScanVector = vector;

        verifyByteScanAgainstRegex(loadRefererSample());
    }

    /** Sanity-check {@link #extractHostByteScan} against the actual regex on real sample data. */
    private static void verifyByteScanAgainstRegex(String[] sampleValues) {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("^https?://(?:www\\.)?([^/]+)/.*$");
        for (String s : sampleValues) {
            java.util.regex.Matcher m = p.matcher(s);
            String expected = m.matches() ? m.replaceFirst("$1") : s;
            String actual = extractHostByteScan(new BytesRef(s)).utf8ToString();
            if (expected.equals(actual) == false) {
                throw new IllegalStateException("byte-scan mismatch for [" + s + "]: expected [" + expected + "] got [" + actual + "]");
            }
        }
    }

    private static Literal keyword(String s) {
        return new Literal(Source.EMPTY, new BytesRef(s), DataType.KEYWORD);
    }

    private static Literal intLit(int i) {
        return new Literal(Source.EMPTY, i, DataType.INTEGER);
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block replace() {
        return evaluator.eval(page);
    }

    /**
     * Non-regex rewrite: CASE/STARTS_WITH/SUBSTRING/LOCATE instead of REPLACE's regex, run as 4
     * sequential EVAL-like steps (each step's result is appended as a page column and referenced by
     * channel in the next step) -- mirrors how real chained EVALs execute, with each intermediate
     * value computed exactly once. No caching involved.
     */
    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block replaceRewrite() {
        Page p0 = page;
        Block schemeStripped = schemeStrippedEvaluator.eval(p0);
        Page p1 = p0.appendBlock(schemeStripped);
        Block wwwStripped = wwwStrippedEvaluator.eval(p1);
        Page p2 = p1.appendBlock(wwwStripped);
        Block slashPos = slashPosEvaluator.eval(p2);
        Page p3 = p2.appendBlock(slashPos);
        return kEvaluator.eval(p3);
    }

    /**
     * Hand-written, single-pass byte-level equivalent of
     * {@code REPLACE(Referer, "^https?://(?:www\.)?([^/]+)/.*$", "$1")} -- no regex engine, no UTF-8
     * decode, no intermediate String/codepoint-counting (unlike {@link Substring}/{@link Locate}, which
     * is why {@link #replaceRewrite} is so much slower). Operates directly on the input {@link BytesRef}
     * bytes; the only allocation on a match is the single output {@link BytesRef} slice. No caching.
     */
    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block replaceByteScan() {
        int positionCount = byteScanVector.getPositionCount();
        try (BytesRefBlock.Builder result = blockFactory.newBytesRefBlockBuilder(positionCount)) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef referer = byteScanVector.getBytesRef(p, scratch);
                result.appendBytesRef(extractHostByteScan(referer));
            }
            return result.build();
        }
    }

    /**
     * Byte-level equivalent of {@code REPLACE(referer, "^https?://(?:www\.)?([^/]+)/.*$", "$1")}.
     * Returns {@code referer} unchanged (matching REPLACE's no-match behavior) if the scheme/slash
     * shape isn't present.
     */
    private static BytesRef extractHostByteScan(BytesRef referer) {
        byte[] b = referer.bytes;
        int off = referer.offset;
        int len = referer.length;

        // "http"
        if (len < 4 || b[off] != 'h' || b[off + 1] != 't' || b[off + 2] != 't' || b[off + 3] != 'p') {
            return referer;
        }
        int pos = 4;
        // optional "s"
        if (pos < len && b[off + pos] == 's') {
            pos++;
        }
        // "://"
        if (pos + 3 > len || b[off + pos] != ':' || b[off + pos + 1] != '/' || b[off + pos + 2] != '/') {
            return referer;
        }
        pos += 3;
        // optional "www."
        if (pos + 4 <= len && b[off + pos] == 'w' && b[off + pos + 1] == 'w' && b[off + pos + 2] == 'w' && b[off + pos + 3] == '.') {
            pos += 4;
        }
        int hostStart = pos;
        int slashIdx = -1;
        for (int i = pos; i < len; i++) {
            if (b[off + i] == '/') {
                slashIdx = i;
                break;
            }
        }
        if (slashIdx <= hostStart) {
            // no trailing slash, or empty host -- regex's [^/]+ / requires >=1 char then a slash
            return referer;
        }
        return new BytesRef(b, off + hostStart, slashIdx - hostStart);
    }

    private static String referer(int id) {
        return "https://www.example" + id + ".com/path/" + id + "?x=" + id;
    }

    private static String[] distinctValues(int n) {
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
            values[i] = referer(i);
        }
        return values;
    }

    private static String[] singleValue(int n) {
        String[] values = new String[n];
        Arrays.fill(values, referer(0));
        return values;
    }

    /**
     * Loads real {@code Referer} values from ClickBench's {@code hits} dataset. Source: 904 non-empty
     * rows (out of 1,000) from {@code ~/.rally/benchmarks/data/clickbench/hits-1k.json.zst}, in original
     * order.
     */
    private static String[] loadRefererSample() {
        List<String> values = new ArrayList<>();
        try (
            InputStream in = ReplaceBenchmark.class.getResourceAsStream("clickbench-referer-sample.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                values.add(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return values.toArray(new String[0]);
    }

    /** Repeats {@code sample} end-to-end to fill {@code n} rows, preserving its internal row order. */
    private static String[] tile(String[] sample, int n) {
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
            values[i] = sample[i % sample.length];
        }
        return values;
    }

    /** Same values and multiplicities as {@code values}, but with row order shuffled (no adjacency). */
    private static String[] shuffle(String[] values, Random random) {
        String[] shuffled = values.clone();
        for (int i = shuffled.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            String tmp = shuffled[i];
            shuffled[i] = shuffled[j];
            shuffled[j] = tmp;
        }
        return shuffled;
    }
}
