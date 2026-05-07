/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Measures the per-block CPU cost of evaluating a {@code WildcardLike} predicate via:
 * <ol>
 *   <li>The reader's late-mat path ({@link ParquetPushedExpressions#evaluateFilter}), which has
 *   a dictionary-aware fast path on {@link OrdinalBytesRefBlock} that runs the automaton
 *   once per dictionary entry plus an int lookup per row.</li>
 *   <li>The downstream {@code FilterExec} path ({@code AutomataMatchEvaluator}), which calls
 *   {@code AutomataMatch.process} per row and runs the automaton on the resolved bytes —
 *   no dictionary fast path even when the input block is ordinal-encoded.</li>
 * </ol>
 *
 * <p>This is the cost asymmetry that matters for the {@code Pushability.YES} vs {@code RECHECK}
 * tradeoff: under YES the reader path runs alone; under RECHECK the FilterExec path runs as
 * well, on top of the survivor block produced by the reader. The test reports both timings and
 * the ratio so the perf vs complexity tradeoff can be evaluated with numbers.
 *
 * <p>This is a perf measurement, not a correctness test. It is gated on the {@code tests.perf}
 * system property so it does not run in CI (where shared infrastructure makes timing noisy and
 * the result is informational only). Run locally with:
 * <pre>
 * ./gradlew :x-pack:plugin:esql-datasource-parquet:test \
 *   --tests "org.elasticsearch.xpack.esql.datasource.parquet.LikeYesVsRecheckCostTests" \
 *   -Dtests.perf=true -Dtests.output=always
 * </pre>
 */
public class LikeYesVsRecheckCostTests extends ESTestCase {

    // Use Elasticsearch's Booleans helper instead of java.lang.Boolean#parseBoolean — the
    // latter is forbidden by the project's forbiddenApis policy because it silently accepts
    // anything that isn't "true" as false (no input validation), which has historically
    // masked typos in system properties.
    private static final boolean PERF_ENABLED = Booleans.parseBoolean(System.getProperty("tests.perf"), false);

    /**
     * Mid-sized block matching what the parquet reader actually emits per page chunk.
     * Picked to mimic a "typical web logs URL column" shape: dictionary of a few hundred
     * unique URLs, hundreds of thousands of rows per block.
     */
    private static final int ROWS = 200_000;
    private static final int DICTIONARY_SIZE = 200;

    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURED_ITERATIONS = 20;

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testYesVsRecheckLikeEvaluationCost() {
        assumeTrue("perf measurement; opt in with -Dtests.perf=true", PERF_ENABLED);

        // Three representative selectivities chosen to bracket the realistic range. The "high
        // selectivity" case (5% match) is the one YES was tuned for — most rows are filtered out
        // and the dictionary fast path saves nearly the entire pattern-match cost. The "high
        // match" case (80%) is the worst case for RECHECK because almost every row survives the
        // reader and gets re-evaluated by FilterExec on the per-row scalar path.
        for (double matchRate : new double[] { 0.05, 0.50, 0.80 }) {
            measureScenario("URL LIKE \"*google*\" matchRate=" + matchRate, "*google*", matchRate);
        }
    }

    private void measureScenario(String label, String pattern, double matchRate) {
        // Build the dictionary so a known fraction of entries match the pattern. Each row gets
        // a uniformly-distributed ordinal, so the survivor count is ~rows*matchRate (with normal
        // statistical jitter, which is small at ROWS=200_000).
        String[] dictionary = buildDictionary(DICTIONARY_SIZE, pattern, matchRate);

        try (
            OrdinalBytesRefBlock ordinalBlock = buildOrdinalBlock(dictionary, ROWS);
            BytesRefBlock plainBlock = materializeAsPlain(ordinalBlock);
        ) {
            Expression urlAttr = attr("url", DataType.KEYWORD);
            ParquetPushedExpressions pushed = new ParquetPushedExpressions(
                List.of(new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern(pattern)))
            );

            // Build the byte-run automaton the FilterExec path would receive at plan time. This
            // is the same conversion AutomataMatch.toEvaluator does, hoisted out of the timing
            // loop so we measure runtime cost, not plan-time cost.
            ByteRunAutomaton automaton = compileAutomaton(pattern);

            // Reader path on the ordinal block (the YES path the optimizer enables today).
            long readerOrdinalNs = timeReaderPath(pushed, ordinalBlock, ROWS);
            // Reader path on a non-dict block (sanity baseline; this is the non-dict-encoded
            // page case that doesn't get the fast path).
            long readerScalarNs = timeReaderPath(pushed, plainBlock, ROWS);
            // FilterExec path on the ordinal block — note this is what would re-evaluate the
            // LIKE under RECHECK on top of the reader's survivor set. We deliberately measure
            // it on ordinal input because the parquet reader emits OrdinalBytesRefBlock for
            // dict-encoded chunks, so this is the realistic survivor-block shape.
            long filterExecOrdinalNs = timeFilterExecPath(automaton, ordinalBlock, ROWS);
            // Same on plain block for completeness.
            long filterExecScalarNs = timeFilterExecPath(automaton, plainBlock, ROWS);

            // Under YES the reader path runs alone. Under RECHECK both run (but FilterExec runs
            // on the survivor block, not the full block). We approximate the survivor cost as
            // matchRate * full block cost since the per-row work is identical.
            double survivorRowCount = ROWS * matchRate;
            long filterExecSurvivorOrdinalNs = (long) (filterExecOrdinalNs * matchRate);
            long filterExecSurvivorScalarNs = (long) (filterExecScalarNs * matchRate);

            logger.info(
                "[{}] rows={} dict={} match={}\n"
                    + "  reader-ordinal (YES path):           {} ns ({} ns/row)\n"
                    + "  reader-scalar  (non-dict baseline):  {} ns ({} ns/row)\n"
                    + "  filterExec-ordinal full block:       {} ns ({} ns/row)\n"
                    + "  filterExec-scalar  full block:       {} ns ({} ns/row)\n"
                    + "  RECHECK extra (ordinal survivors ~{} rows): {} ns ({} ns/row)\n"
                    + "  RECHECK extra (scalar  survivors ~{} rows): {} ns ({} ns/row)\n"
                    + "  YES vs RECHECK total ordinal: {} ns vs {} ns ({}x more under RECHECK)\n"
                    + "  YES vs RECHECK total scalar:  {} ns vs {} ns ({}x more under RECHECK)",
                label,
                ROWS,
                DICTIONARY_SIZE,
                matchRate,
                readerOrdinalNs,
                fmtRate(readerOrdinalNs, ROWS),
                readerScalarNs,
                fmtRate(readerScalarNs, ROWS),
                filterExecOrdinalNs,
                fmtRate(filterExecOrdinalNs, ROWS),
                filterExecScalarNs,
                fmtRate(filterExecScalarNs, ROWS),
                (long) survivorRowCount,
                filterExecSurvivorOrdinalNs,
                fmtRate(filterExecSurvivorOrdinalNs, Math.max(1, (long) survivorRowCount)),
                (long) survivorRowCount,
                filterExecSurvivorScalarNs,
                fmtRate(filterExecSurvivorScalarNs, Math.max(1, (long) survivorRowCount)),
                readerOrdinalNs,
                readerOrdinalNs + filterExecSurvivorOrdinalNs,
                fmtRatio(readerOrdinalNs + filterExecSurvivorOrdinalNs, readerOrdinalNs),
                readerScalarNs,
                readerScalarNs + filterExecSurvivorScalarNs,
                fmtRatio(readerScalarNs + filterExecSurvivorScalarNs, readerScalarNs)
            );
        }
    }

    private long timeReaderPath(ParquetPushedExpressions pushed, Block block, int rows) {
        // Force a fresh WordMask each iteration to avoid the prior result leaking into the
        // measurement (the API mutates the WordMask in place).
        Map<String, Block> blocks = Map.of("url", block);
        WordMask reusable = new WordMask();
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            pushed.evaluateFilter(blocks, rows, reusable);
        }
        long[] samples = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long start = System.nanoTime();
            pushed.evaluateFilter(blocks, rows, reusable);
            samples[i] = System.nanoTime() - start;
        }
        return median(samples);
    }

    private long timeFilterExecPath(ByteRunAutomaton automaton, Block block, int rows) {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            runFilterExecLoop(automaton, block, rows);
        }
        long[] samples = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long start = System.nanoTime();
            runFilterExecLoop(automaton, block, rows);
            samples[i] = System.nanoTime() - start;
        }
        return median(samples);
    }

    /**
     * Faithfully replays the inner loop of {@code AutomataMatchEvaluator#eval(int, BytesRefVector)}
     * (and the BytesRefBlock path), which is what FilterExec invokes when re-checking a LIKE.
     * Using the real evaluator would drag in {@code DriverContext}, {@code BlockFactory} for the
     * boolean output, and warning machinery — all noise relative to the actual work, which is
     * the per-row {@code automaton.run(bytes, offset, length)} call.
     */
    private static void runFilterExecLoop(ByteRunAutomaton automaton, Block block, int rows) {
        int survivors = 0;
        BytesRef scratch = new BytesRef();
        if (block instanceof OrdinalBytesRefBlock obb) {
            BytesRefVector dict = obb.getDictionaryVector();
            IntBlock ordinals = obb.getOrdinalsBlock();
            for (int p = 0; p < rows; p++) {
                if (ordinals.isNull(p)) {
                    continue;
                }
                int ordinal = ordinals.getInt(ordinals.getFirstValueIndex(p));
                BytesRef val = dict.getBytesRef(ordinal, scratch);
                if (automaton.run(val.bytes, val.offset, val.length)) {
                    survivors++;
                }
            }
        } else {
            BytesRefBlock bb = (BytesRefBlock) block;
            for (int p = 0; p < rows; p++) {
                if (bb.isNull(p)) {
                    continue;
                }
                BytesRef val = bb.getBytesRef(bb.getFirstValueIndex(p), scratch);
                if (automaton.run(val.bytes, val.offset, val.length)) {
                    survivors++;
                }
            }
        }
        // Use the result so the JIT doesn't dead-code the loop.
        if (survivors < 0) {
            throw new AssertionError("unreachable");
        }
    }

    private static ByteRunAutomaton compileAutomaton(String pattern) {
        // Replicates AutomataMatch.toEvaluator's pipeline: build the regex automaton, convert
        // utf32 -> utf8, determinize, wrap in ByteRunAutomaton. We do it once outside the timing
        // loop because plan-time cost isn't part of the YES vs RECHECK debate.
        var wp = new WildcardPattern(pattern);
        var u32 = wp.createAutomaton(true);
        var u8 = new UTF32ToUTF8().convert(u32);
        var det = Operations.determinize(u8, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        return new ByteRunAutomaton(det, true);
    }

    private static String[] buildDictionary(int size, String pattern, double matchRate) {
        // We want exactly round(size * matchRate) dict entries to match the pattern. The pattern
        // is "*google*" so a matching entry contains "google", a non-matching entry doesn't.
        String marker = pattern.replace("*", "").replace("?", "x");
        int matchCount = (int) Math.round(size * matchRate);
        String[] out = new String[size];
        Random r = new Random(42);
        for (int i = 0; i < size; i++) {
            String prefix = "https://example.com/path-" + i + "/";
            String suffix = "?id=" + r.nextInt(10_000);
            if (i < matchCount) {
                out[i] = prefix + marker + suffix;
            } else {
                // Make sure the marker can't appear by accident — pad the middle with "x".
                out[i] = prefix + "xxxx" + suffix;
            }
        }
        return out;
    }

    private OrdinalBytesRefBlock buildOrdinalBlock(String[] dictionary, int rows) {
        BytesRefVector dictVector = null;
        IntBlock ordinalsBlock = null;
        boolean ok = false;
        try (BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(dictionary.length)) {
            for (String s : dictionary) {
                dictBuilder.appendBytesRef(new BytesRef(s));
            }
            dictVector = dictBuilder.build();
            try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(rows)) {
                Random r = new Random(7);
                for (int i = 0; i < rows; i++) {
                    b.appendInt(r.nextInt(dictionary.length));
                }
                ordinalsBlock = b.build();
            }
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalsBlock, dictVector);
            ok = true;
            return result;
        } finally {
            if (ok == false) {
                if (ordinalsBlock != null) ordinalsBlock.close();
                if (dictVector != null) dictVector.close();
            }
        }
    }

    private BytesRefBlock materializeAsPlain(OrdinalBytesRefBlock src) {
        BytesRef scratch = new BytesRef();
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(src.getPositionCount())) {
            for (int i = 0; i < src.getPositionCount(); i++) {
                if (src.isNull(i)) {
                    b.appendNull();
                } else {
                    b.appendBytesRef(src.getBytesRef(i, scratch));
                }
            }
            return b.build();
        }
    }

    private static long median(long[] samples) {
        long[] copy = samples.clone();
        Arrays.sort(copy);
        return copy[copy.length / 2];
    }

    private static String fmtRate(long ns, long rows) {
        return String.format(Locale.ROOT, "%.2f", (double) ns / rows);
    }

    private static String fmtRatio(long num, long den) {
        return String.format(Locale.ROOT, "%.2f", (double) num / den);
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
