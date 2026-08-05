/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HighlightConfig;
import org.elasticsearch.compute.operator.HighlightOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.HighlightOptions;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures runtime {@code MATCH} and {@code HIGHLIGHT} cost per row.
 *
 * <p>The highlight scenarios cover one hit, one miss, and enough hits to produce multiple fragments. The combined
 * scenario runs {@code MATCH} followed by {@code HIGHLIGHT}. All highlight scenarios use the command defaults.
 *
 * <p>Rows contain deterministic random text split into sentences. {@link #setup()} checks the expected matches and
 * snippets before JMH starts measuring.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class HighlightAtRuntimeBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    // Keep pages small enough to run many passes for the largest text size.
    // At 128 rows a single run() invocation is ~170 ms, so several invocations still fit in each 1s measurement iteration. At 1024 rows one
    // invocation would exceed the 1s window (leaving too few passes per iteration) and allocate >1 GB, so the smaller page keeps sampling
    // and GC pressure reasonable.

    private static final int BLOCK_LENGTH = 128;
    private static final String FIELD = "content";

    /** Present in every row. */
    private static final String TERM = "fox";
    /** Absent from every row. */
    private static final String MISS_TERM = "cat";

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    /** Number of tokens in each row. */
    @Param({ "16", "128", "1024", "8192" })
    public int textTokens;

    /** Operation under test. */
    @Param({ "match", "highlightHit", "highlightMiss", "highlightHitMany", "matchHighlightHit" })
    public String scenario;

    private ExpressionEvaluator matchEvaluator;
    private HighlightOperator highlightOperator;
    private boolean highlightHits;
    private int occurrences;
    private Page page;

    @Setup(Level.Trial)
    public void setup() {
        occurrences = "highlightHitMany".equals(scenario) ? Math.max(2, textTokens / 64) : 1;
        Random random = new Random(42L);
        var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendBytesRef(new BytesRef(row(random)));
        }
        page = new Page(builder.build().asBlock());

        switch (scenario) {
            case "match" -> matchEvaluator = newMatchEvaluator();
            case "highlightHit", "highlightHitMany" -> {
                highlightHits = true;
                highlightOperator = newHighlightOperator(TERM);
            }
            case "highlightMiss" -> {
                highlightHits = false;
                highlightOperator = newHighlightOperator(MISS_TERM);
            }
            case "matchHighlightHit" -> {
                highlightHits = true;
                matchEvaluator = newMatchEvaluator();
                highlightOperator = newHighlightOperator(TERM);
            }
            default -> throw new IllegalArgumentException("unknown scenario: " + scenario);
        }

        validateSetup();
    }

    private ExpressionEvaluator newMatchEvaluator() {
        Attribute field = new ReferenceAttribute(Source.EMPTY, FIELD, DataType.TEXT);
        Expression matchExpr = new Match(Source.EMPTY, field, Literal.text(Source.EMPTY, TERM), null);
        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        return EvalMapper.toEvaluator(FOLD_CONTEXT, matchExpr, layoutBuilder.build()).get(driverContext);
    }

    private HighlightOperator newHighlightOperator(String highlightTerm) {
        Analyzer analyzer = new StandardAnalyzer();
        Query query = new TermQuery(new Term(FIELD, highlightTerm));
        // Reuse the command defaults from HighlightOptions.
        HighlightOptions options = HighlightOptions.from(null, FOLD_CONTEXT);
        HighlightConfig config = new HighlightConfig(
            highlightTerm,
            options.preTag(),
            options.postTag(),
            options.encoder(),
            options.numberOfFragments(),
            options.fragmentSize(),
            options.noMatchSize(),
            HighlightOptions.BOUNDARY_SCANNER_WORD.equals(options.boundaryScanner()),
            options.boundaryScannerLocale(),
            HighlightOptions.ORDER_SCORE.equals(options.order()),
            options.analyzerName(),
            options.maxAnalyzedOffset()
        ).withExecutionContext(analyzer, query, List.of(FIELD));
        return new HighlightOperator(blockFactory, config, new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) });
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        page.releaseBlocks();
        if (matchEvaluator != null) {
            matchEvaluator.close();
        }
        if (highlightOperator != null) {
            highlightOperator.close();
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public void run(Blackhole bh) {
        if (matchEvaluator != null) {
            try (Block matched = matchEvaluator.eval(page)) {
                bh.consume(matched);
            }
        }
        if (highlightOperator != null) {
            highlightOperator.addInput(page);
            bh.consume(highlightOperator.getOutput());
        }
    }

    /**
     * Builds a row of random tokens with {@link #TERM} inserted at random positions. Sentences start with a capital
     * because the UAX#29 rules do not break before a lowercase letter.
     */
    private String row(Random random) {
        String[] tokens = new String[textTokens];
        for (int i = 0; i < textTokens; i++) {
            tokens[i] = randomToken(random);
        }
        for (int placed = 0; placed < occurrences;) {
            int position = random.nextInt(textTokens);
            if (TERM.equals(tokens[position]) == false) {
                tokens[position] = TERM;
                placed++;
            }
        }
        StringBuilder text = new StringBuilder();
        boolean sentenceStart = true;
        int remaining = sentenceLength(random);
        for (int i = 0; i < textTokens; i++) {
            if (i > 0) {
                text.append(' ');
            }
            String token = tokens[i];
            if (sentenceStart) {
                text.append(Character.toUpperCase(token.charAt(0))).append(token, 1, token.length());
                sentenceStart = false;
            } else {
                text.append(token);
            }
            if (--remaining == 0) {
                text.append('.');
                remaining = sentenceLength(random);
                sentenceStart = true;
            }
        }
        return text.toString();
    }

    private static int sentenceLength(Random random) {
        return 8 + random.nextInt(9);
    }

    private static String randomToken(Random random) {
        int length = 4 + random.nextInt(2);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    private void validateSetup() {
        if (matchEvaluator != null) {
            try (BooleanBlock matched = (BooleanBlock) matchEvaluator.eval(page)) {
                int trueCount = 0;
                for (int i = 0; i < matched.getPositionCount(); i++) {
                    if (matched.isNull(i) == false && matched.getBoolean(matched.getFirstValueIndex(i))) {
                        trueCount++;
                    }
                }
                if (trueCount != BLOCK_LENGTH) {
                    throw new AssertionError(
                        "match self-test failed for textTokens ["
                            + textTokens
                            + "]: expected ["
                            + BLOCK_LENGTH
                            + "] matched rows but found ["
                            + trueCount
                            + "]"
                    );
                }
            }
        }

        if (highlightOperator == null) {
            return;
        }
        int expected = highlightHits ? BLOCK_LENGTH : 0;
        highlightOperator.addInput(page);
        Page output = highlightOperator.getOutput();
        BytesRefBlock highlighted = output.getBlock(output.getBlockCount() - 1);
        BytesRef scratch = new BytesRef();
        int nonNull = 0;
        int multiFragmentRows = 0;
        boolean sawTag = false;
        for (int i = 0; i < highlighted.getPositionCount(); i++) {
            if (highlighted.isNull(i) == false) {
                nonNull++;
                if (highlighted.getValueCount(i) > 1) {
                    multiFragmentRows++;
                }
                sawTag |= highlighted.getBytesRef(highlighted.getFirstValueIndex(i), scratch).utf8ToString().contains("<em>");
            }
        }
        if (nonNull != expected || (expected > 0 && sawTag == false)) {
            throw new AssertionError(
                "highlight self-test failed for scenario ["
                    + scenario
                    + "] textTokens ["
                    + textTokens
                    + "]: expected ["
                    + expected
                    + "] snippets (with <em> tag when > 0) but found ["
                    + nonNull
                    + "] non-null, sawTag="
                    + sawTag
            );
        }
        if ("highlightHitMany".equals(scenario) && textTokens >= 1024 && multiFragmentRows == 0) {
            throw new AssertionError(
                "highlight self-test failed for scenario ["
                    + scenario
                    + "] textTokens ["
                    + textTokens
                    + "]: expected multi-fragment rows but every row produced a single fragment"
            );
        }
        // Release only the appended output block; block 0 is the shared input block reused by the benchmark loop.
        highlighted.close();
    }
}
