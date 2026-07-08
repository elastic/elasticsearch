/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end execution tests for runtime {@code match}, where the field is
 * not a Lucene-mapped index field). Unlike {@link MatchTests}, which only checks type resolution and serialization,
 * this builds the actual runtime evaluators and runs them over real {@link Block}s.
 * <p>
 * It covers the two behaviors of runtime match: analyzed full-text matching on a {@code text} field (the
 * {@code to_text(...)} case), and exact value matching on every other type. Multivalue (any-value match), null/missing
 * positions, and the thread-safety of the shared per-thread scratch {@link BytesRef} are exercised too.
 */
public class MatchRuntimeSearchEvaluatorTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

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

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    private static Match runtimeMatch(DataType fieldType, Object queryValue, DataType queryType) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", fieldType);
        Literal query = new Literal(Source.EMPTY, queryValue, queryType);
        Match match = new Match(Source.EMPTY, field, query, null, EsqlTestUtils.TEST_CFG);
        assertTrue("expected a runtime search, not a pushed-down query", match.isRuntimeSearch());
        return match;
    }

    /**
     * Builds the runtime evaluator for {@code match} and runs it over a single field block (built from the breaking
     * block factory so {@link #allMemoryReleased()} verifies nothing leaks), returning the per-position boolean
     * results ({@code null} only if a position could not be evaluated, which never happens for these inputs).
     */
    private Boolean[] evaluate(Match match, Function<BlockFactory, Block> fieldBuilder) {
        DriverContext context = driverContext();
        Block fieldBlock = fieldBuilder.apply(context.blockFactory());
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

    private static Block bytesRefBlock(BlockFactory factory, Consumer<BytesRefBlock.Builder> build) {
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(4)) {
            build.accept(builder);
            return builder.build();
        }
    }

    // ---- text: analyzed full-text matching (the to_text case) ----

    public void testTextIsAnalyzed() {
        // "brown" matches "Brown" (standard analyzer lowercases) on the first row; no row mentions a dog.
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("brown"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("This is a Brown fox"));
            builder.appendBytesRef(new BytesRef("The cat sat on the mat"));
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextMatchesAnyTokenWithOrSemantics() {
        // Multi-term query uses OR semantics on the runtime path, so a single shared token is enough to match.
        Boolean[] result = evaluate(
            runtimeMatch(TEXT, new BytesRef("quick turtle"), KEYWORD),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a quick fox"));
                builder.appendBytesRef(new BytesRef("a slow turtle"));
            })
        );
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testTextMultiValueAndNull() {
        // Matches if any value in the position matches; a missing value never matches.
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("cat"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("brown fox"));
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    // ---- keyword: exact (unanalyzed) matching ----

    public void testKeywordIsExactAndCaseSensitive() {
        // Unlike text, keyword compares the whole value byte-for-byte: only the exact "hello" matches.
        Boolean[] result = evaluate(runtimeMatch(KEYWORD, new BytesRef("hello"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("Hello"));
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("hell"));
        }));
        assertArrayEquals(new Boolean[] { false, true, false }, result);
    }

    public void testKeywordMultiValueAndNull() {
        Boolean[] result = evaluate(runtimeMatch(KEYWORD, new BytesRef("b"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    // ---- numeric / boolean: exact value matching ----

    public void testLong() {
        Boolean[] result = evaluate(runtimeMatch(LONG, 30L, LONG), factory -> {
            try (var builder = factory.newLongBlockBuilder(3)) {
                builder.appendLong(10L);
                builder.beginPositionEntry();
                builder.appendLong(20L);
                builder.appendLong(30L);
                builder.endPositionEntry();
                builder.appendNull();
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { false, true, false }, result);
    }

    public void testInteger() {
        Boolean[] result = evaluate(runtimeMatch(INTEGER, 7, INTEGER), factory -> {
            try (var builder = factory.newIntBlockBuilder(2)) {
                builder.appendInt(7);
                builder.appendInt(8);
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testDouble() {
        Boolean[] result = evaluate(runtimeMatch(DOUBLE, 2.5d, DOUBLE), factory -> {
            try (var builder = factory.newDoubleBlockBuilder(2)) {
                builder.appendDouble(1.5);
                builder.appendDouble(2.5);
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { false, true }, result);
    }

    public void testBoolean() {
        Boolean[] result = evaluate(runtimeMatch(BOOLEAN, true, BOOLEAN), factory -> {
            try (var builder = factory.newBooleanBlockBuilder(3)) {
                builder.appendBoolean(true);
                builder.appendBoolean(false);
                builder.appendNull();
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    /**
     * The {@code bytes_ref} evaluator uses a per-evaluator scratch {@link BytesRef} (created per {@link DriverContext}
     * via a {@code THREAD_LOCAL}-scoped {@code @Fixed}). Many threads sharing one factory must each get an independent
     * scratch, so concurrent evaluation cannot corrupt the comparison. Run the same match on many threads and check
     * every result.
     */
    public void testScratchIsThreadSafe() {
        Match match = runtimeMatch(KEYWORD, new BytesRef("needle"), KEYWORD);
        ExpressionEvaluator.Factory factory = match.toEvaluator(toEvaluator());

        runInParallel(64, task -> {
            boolean expectMatch = (task & 1) == 0;
            DriverContext context = driverContext();
            Block field = bytesRefBlock(
                context.blockFactory(),
                builder -> builder.appendBytesRef(new BytesRef(expectMatch ? "needle" : "haystack"))
            );
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
