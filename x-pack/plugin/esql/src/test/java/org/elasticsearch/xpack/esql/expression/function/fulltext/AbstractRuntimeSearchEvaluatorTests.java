/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

/**
 * Shared harness for end-to-end execution tests of runtime full-text functions, where the field is not a
 * Lucene-mapped index field. Subclasses build the actual runtime evaluators for their function and run them over
 * real {@link Block}s built from a breaking block factory, so {@link #allMemoryReleased()} verifies nothing leaks.
 */
public abstract class AbstractRuntimeSearchEvaluatorTests extends ESTestCase {

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

    protected static EvaluatorMapper.ToEvaluator toEvaluator() {
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

    protected DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    /**
     * Builds the runtime evaluator for the given full-text function and runs it over a single field block, returning
     * the per-position boolean results ({@code null} only if a position could not be evaluated).
     */
    protected Boolean[] evaluate(FullTextFunction function, Function<BlockFactory, Block> fieldBuilder) {
        DriverContext context = driverContext();
        Block fieldBlock = fieldBuilder.apply(context.blockFactory());
        ExpressionEvaluator.Factory factory = function.toEvaluator(toEvaluator());
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

    protected static Block bytesRefBlock(BlockFactory factory, Consumer<BytesRefBlock.Builder> build) {
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(4)) {
            build.accept(builder);
            return builder.build();
        }
    }
}
