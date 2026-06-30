/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * End-to-end execution tests for runtime {@code match} (the {@code runtime_lexical_search} path, where the field is
 * not a Lucene-mapped index field). Unlike {@link MatchTests}, which only checks type resolution and serialization,
 * this builds the actual runtime evaluators and runs them over real {@link Block}s.
 * <p>
 * It covers the two behaviors of runtime match: analyzed full-text matching on a {@code text} field (the
 * {@code to_text(...)} case), and exact value matching on every other type. Multivalue (any-value match), null/missing
 * positions, and the thread-safety of the shared per-thread scratch {@link BytesRef} are exercised too.
 */
public class MatchRuntimeSearchEvaluatorTests extends ESTestCase {

    private static final Configuration RUNTIME_CONFIG = configuration(
        new QueryPragmas(Settings.builder().put(QueryPragmas.RUNTIME_LEXICAL_SEARCH.getKey(), true).build())
    );

    /** A field evaluator that simply hands back the block at channel 0 of the page. */
    private static final ExpressionEvaluator.Factory FIELD_AT_0 = context -> new ExpressionEvaluator() {
        @Override
        public Block eval(Page page) {
            Block block = page.getBlock(0);
            block.incRef();
            return block;
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    };

    private static EvaluatorMapper.ToEvaluator toEvaluator() {
        return new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return FIELD_AT_0;
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
    }

    private static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
    }

    private static Match runtimeMatch(DataType fieldType, Object queryValue, DataType queryType) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", fieldType);
        Literal query = new Literal(Source.EMPTY, queryValue, queryType);
        Match match = new Match(Source.EMPTY, field, query, null, RUNTIME_CONFIG);
        assertTrue("expected a runtime search, not a pushed-down query", match.isRuntimeSearch());
        return match;
    }

    /**
     * Builds the runtime evaluator for {@code match} and runs it over a single field block, returning the per-position
     * boolean results ({@code null} only if a position could not be evaluated, which never happens for these inputs).
     */
    private static Boolean[] evaluate(Match match, Block fieldBlock) {
        DriverContext context = driverContext();
        ExpressionEvaluator.Factory factory = match.toEvaluator(toEvaluator());
        try (ExpressionEvaluator evaluator = factory.get(context)) {
            Page page = new Page(fieldBlock);
            try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                Boolean[] out = new Boolean[result.getPositionCount()];
                for (int p = 0; p < out.length; p++) {
                    out[p] = result.isNull(p) ? null : result.getBoolean(result.getFirstValueIndex(p));
                }
                return out;
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static Block bytesRefBlock(Consumer<BytesRefBlock.Builder> build) {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(4)) {
            build.accept(builder);
            return builder.build();
        }
    }

    private void assumeRuntimeSearchSupported() {
        assumeTrue("requires the runtime match capability to be enabled", EsqlCapabilities.Cap.MATCH_RUNTIME_SEARCH.isEnabled());
    }

    // ---- text: analyzed full-text matching (the to_text case) ----

    public void testTextIsAnalyzed() {
        assumeRuntimeSearchSupported();
        Block field = bytesRefBlock(builder -> {
            builder.appendBytesRef(new BytesRef("This is a Brown fox"));
            builder.appendBytesRef(new BytesRef("The cat sat on the mat"));
        });
        // "brown" matches "Brown" (standard analyzer lowercases) on the first row; no row mentions a dog.
        assertArrayEquals(new Boolean[] { true, false }, evaluate(runtimeMatch(TEXT, new BytesRef("brown"), KEYWORD), field));
    }

    public void testTextMatchesAnyTokenWithOrSemantics() {
        assumeRuntimeSearchSupported();
        Block field = bytesRefBlock(builder -> {
            builder.appendBytesRef(new BytesRef("a quick fox"));
            builder.appendBytesRef(new BytesRef("a slow turtle"));
        });
        // Multi-term query uses OR semantics on the runtime path, so a single shared token is enough to match.
        assertArrayEquals(new Boolean[] { true, true }, evaluate(runtimeMatch(TEXT, new BytesRef("quick turtle"), KEYWORD), field));
    }

    public void testTextMultiValueAndNull() {
        assumeRuntimeSearchSupported();
        Block field = bytesRefBlock(builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("brown fox"));
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.endPositionEntry();
            builder.appendNull();
        });
        // Matches if any value in the position matches; a missing value never matches.
        assertArrayEquals(new Boolean[] { true, false }, evaluate(runtimeMatch(TEXT, new BytesRef("cat"), KEYWORD), field));
    }

    // ---- keyword: exact (unanalyzed) matching ----

    public void testKeywordIsExactAndCaseSensitive() {
        assumeRuntimeSearchSupported();
        Block field = bytesRefBlock(builder -> {
            builder.appendBytesRef(new BytesRef("Hello"));
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("hell"));
        });
        // Unlike text, keyword compares the whole value byte-for-byte: only the exact "hello" matches.
        assertArrayEquals(new Boolean[] { false, true, false }, evaluate(runtimeMatch(KEYWORD, new BytesRef("hello"), KEYWORD), field));
    }

    public void testKeywordMultiValueAndNull() {
        assumeRuntimeSearchSupported();
        Block field = bytesRefBlock(builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();
            builder.appendNull();
        });
        assertArrayEquals(new Boolean[] { true, false }, evaluate(runtimeMatch(KEYWORD, new BytesRef("b"), KEYWORD), field));
    }

    // ---- numeric / boolean: exact value matching ----

    public void testLong() {
        assumeRuntimeSearchSupported();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block field;
        try (var builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(10L);
            builder.beginPositionEntry();
            builder.appendLong(20L);
            builder.appendLong(30L);
            builder.endPositionEntry();
            builder.appendNull();
            field = builder.build();
        }
        assertArrayEquals(new Boolean[] { false, true, false }, evaluate(runtimeMatch(LONG, 30L, LONG), field));
    }

    public void testInteger() {
        assumeRuntimeSearchSupported();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block field;
        try (var builder = blockFactory.newIntBlockBuilder(2)) {
            builder.appendInt(7);
            builder.appendInt(8);
            field = builder.build();
        }
        assertArrayEquals(new Boolean[] { true, false }, evaluate(runtimeMatch(INTEGER, 7, INTEGER), field));
    }

    public void testDouble() {
        assumeRuntimeSearchSupported();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block field;
        try (var builder = blockFactory.newDoubleBlockBuilder(2)) {
            builder.appendDouble(1.5);
            builder.appendDouble(2.5);
            field = builder.build();
        }
        assertArrayEquals(new Boolean[] { false, true }, evaluate(runtimeMatch(DOUBLE, 2.5d, DOUBLE), field));
    }

    public void testBoolean() {
        assumeRuntimeSearchSupported();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block field;
        try (var builder = blockFactory.newBooleanBlockBuilder(3)) {
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.appendNull();
            field = builder.build();
        }
        assertArrayEquals(new Boolean[] { true, false, false }, evaluate(runtimeMatch(BOOLEAN, true, BOOLEAN), field));
    }

    /**
     * The {@code bytes_ref} evaluator uses a per-evaluator scratch {@link BytesRef} (created per {@link DriverContext}
     * via a {@code THREAD_LOCAL}-scoped {@code @Fixed}). Many threads sharing one factory must each get an independent
     * scratch, so concurrent evaluation cannot corrupt the comparison. Run the same match on many threads and check
     * every result.
     */
    public void testScratchIsThreadSafe() {
        assumeRuntimeSearchSupported();
        Match match = runtimeMatch(KEYWORD, new BytesRef("needle"), KEYWORD);
        ExpressionEvaluator.Factory factory = match.toEvaluator(toEvaluator());

        runInParallel(64, task -> {
            boolean expectMatch = (task & 1) == 0;
            Block field = bytesRefBlock(builder -> builder.appendBytesRef(new BytesRef(expectMatch ? "needle" : "haystack")));
            DriverContext context = driverContext();
            try (ExpressionEvaluator evaluator = factory.get(context)) {
                Page page = new Page(field);
                try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                    assertEquals("task " + task, expectMatch, result.getBoolean(0));
                } finally {
                    page.releaseBlocks();
                }
            }
        });
    }
}
