/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEqualsMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

public final class EvalMapper {

    private static final List<ExpressionMapper<?>> MAPPERS = List.of(
        new InsensitiveEqualsMapper(),
        new BooleanLogic(),
        new Attributes(),
        new Literals()
    );

    private EvalMapper() {}

    public static ExpressionEvaluator.Factory toEvaluator(ExpressionContext ctx, Expression exp, Layout layout) {
        return toEvaluator(ctx, exp, layout, EmptyIndexedByShardId.instance());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    /**
     * Provides an ExpressionEvaluator factory to evaluate an expression.
     *
     * @param foldCtx the fold context for folding expressions
     * @param exp the expression to generate an evaluator for
     * @param layout the mapping from attributes to channels
     * @param shardContexts the shard contexts, needed to generate queries for expressions that couldn't be pushed down to Lucene
     */
    public static ExpressionEvaluator.Factory toEvaluator(
        ExpressionContext ctx,
        Expression exp,
        Layout layout,
        IndexedByShardId<? extends ShardContext> shardContexts
    ) {
        if (exp instanceof EvaluatorMapper m) {
            return m.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression expression) {
                    return toEvaluator(ctx, expression, layout, shardContexts);
                }

                @Override
                public Configuration configuration() {
                    return ctx.configuration();
                }

                @Override
                public FoldContext foldCtx() {
                    return ctx.foldCtx();
                }

                @Override
                public IndexedByShardId<? extends ShardContext> shardContexts() {
                    return shardContexts;
                }
            });
        }
        for (ExpressionMapper em : MAPPERS) {
            if (em.typeToken.isInstance(exp)) {
                return em.map(ctx, exp, layout, shardContexts);
            }
        }
        throw new QlIllegalArgumentException("Unsupported expression [{}]", exp);
    }

    static class BooleanLogic extends ExpressionMapper<BinaryLogic> {
        @Override
        public ExpressionEvaluator.Factory map(
            ExpressionContext ctx,
            BinaryLogic bc,
            Layout layout,
            IndexedByShardId<? extends ShardContext> shardContexts
        ) {
            var leftEval = toEvaluator(ctx, bc.left(), layout, shardContexts);
            var rightEval = toEvaluator(ctx, bc.right(), layout, shardContexts);
            /**
             * Evaluator for the <href a="https://en.wikipedia.org/wiki/Three-valued_logic">three-valued boolean expressions</href>.
             * We can't generate these with the {@link Evaluator} annotation because that
             * always implements viral null. And three-valued boolean expressions don't.
             * {@code false AND null} is {@code false} and {@code true OR null} is {@code true}.
             */
            record BooleanLogicExpressionEvaluator(BinaryLogic bl, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {

                private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(
                    BooleanLogicExpressionEvaluator.class
                );

                @Override
                public Block eval(Page page) {
                    try (Block lhs = leftEval.eval(page); Block rhs = rightEval.eval(page)) {
                        Vector lhsVector = lhs.asVector();
                        Vector rhsVector = rhs.asVector();
                        if (lhsVector != null && rhsVector != null) {
                            return eval((BooleanVector) lhsVector, (BooleanVector) rhsVector);
                        }
                        return eval(lhs, rhs);
                    }
                }

                @Override
                public long baseRamBytesUsed() {
                    return BASE_RAM_BYTES_USED;
                }

                /**
                 * Eval blocks, handling {@code null}. This takes {@link Block} instead of
                 * {@link BooleanBlock} because blocks that <strong>only</strong> contain
                 * {@code null} can't be cast to {@link BooleanBlock}. So we check for
                 * {@code null} first and don't cast at all if the value is {@code null}.
                 */
                private Block eval(Block lhs, Block rhs) {
                    int positionCount = lhs.getPositionCount();
                    try (BooleanBlock.Builder result = lhs.blockFactory().newBooleanBlockBuilder(positionCount)) {
                        for (int p = 0; p < positionCount; p++) {
                            if (lhs.getValueCount(p) > 1) {
                                result.appendNull();
                                continue;
                            }
                            if (rhs.getValueCount(p) > 1) {
                                result.appendNull();
                                continue;
                            }
                            Boolean v = bl.function()
                                .apply(
                                    lhs.isNull(p) ? null : ((BooleanBlock) lhs).getBoolean(lhs.getFirstValueIndex(p)),
                                    rhs.isNull(p) ? null : ((BooleanBlock) rhs).getBoolean(rhs.getFirstValueIndex(p))
                                );
                            if (v == null) {
                                result.appendNull();
                                continue;
                            }
                            result.appendBoolean(v);
                        }
                        return result.build();
                    }
                }

                private Block eval(BooleanVector lhs, BooleanVector rhs) {
                    int positionCount = lhs.getPositionCount();
                    try (var result = lhs.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
                        for (int p = 0; p < positionCount; p++) {
                            result.appendBoolean(p, bl.function().apply(lhs.getBoolean(p), rhs.getBoolean(p)));
                        }
                        return result.build().asBlock();
                    }
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(leftEval, rightEval);
                }
            }
            return new ExpressionEvaluator.Factory() {
                @Override
                public ExpressionEvaluator get(DriverContext driverContext) {
                    return new BooleanLogicExpressionEvaluator(bc, leftEval.get(driverContext), rightEval.get(driverContext));
                }

                @Override
                public String toString() {
                    return "BooleanLogicExpressionEvaluator[" + "bl=" + bc + ", leftEval=" + leftEval + ", rightEval=" + rightEval + ']';
                }
            };
        }
    }

    static class Attributes extends ExpressionMapper<Attribute> {
        @Override
        public ExpressionEvaluator.Factory map(
            ExpressionContext ctx,
            Attribute attr,
            Layout layout,
            IndexedByShardId<? extends ShardContext> shardContexts
        ) {
            record Attribute(int channel) implements ExpressionEvaluator {
                private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Attribute.class);

                @Override
                public Block eval(Page page) {
                    Block block = page.getBlock(channel);
                    block.incRef();
                    return block;
                }

                @Override
                public long baseRamBytesUsed() {
                    return BASE_RAM_BYTES_USED;
                }

                @Override
                public void close() {}
            }
            record AttributeFactory(int channel) implements ExpressionEvaluator.Factory {
                @Override
                public ExpressionEvaluator get(DriverContext driverContext) {
                    return new Attribute(channel);
                }

                @Override
                public String toString() {
                    return "Attribute[channel=" + channel + "]";
                }

                @Override
                public boolean eagerEvalSafeInLazy() {
                    return true;
                }
            }
            return new AttributeFactory(layout.get(attr.id()).channel());
        }
    }

    static class Literals extends ExpressionMapper<Literal> {

        @Override
        public ExpressionEvaluator.Factory map(
            ExpressionContext ctx,
            Literal lit,
            Layout layout,
            IndexedByShardId<? extends ShardContext> shardContexts
        ) {
            record LiteralsEvaluator(DriverContext context, Literal lit) implements ExpressionEvaluator {
                private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LiteralsEvaluator.class);

                @Override
                public Block eval(Page page) {
                    return block(lit, context.blockFactory(), page.getPositionCount());
                }

                @Override
                public String toString() {
                    return "LiteralsEvaluator[lit=" + lit + ']';
                }

                @Override
                public long baseRamBytesUsed() {
                    return BASE_RAM_BYTES_USED + lit.ramBytesUsed();
                }

                @Override
                public void close() {}
            }
            record LiteralsEvaluatorFactory(Literal lit) implements ExpressionEvaluator.Factory {
                @Override
                public ExpressionEvaluator get(DriverContext driverContext) {
                    return new LiteralsEvaluator(driverContext, lit);
                }

                @Override
                public String toString() {
                    return "LiteralsEvaluator[lit=" + lit + "]";
                }

                @Override
                public boolean eagerEvalSafeInLazy() {
                    return true;
                }
            }
            return new LiteralsEvaluatorFactory(lit);
        }

        private static Block block(Literal lit, BlockFactory blockFactory, int positions) {
            var value = lit.value();
            if (value == null) {
                return blockFactory.newConstantNullBlock(positions);
            }

            if (value instanceof List<?> multiValue) {
                if (multiValue.isEmpty()) {
                    return blockFactory.newConstantNullBlock(positions);
                }
                var wrapper = BlockUtils.wrapperFor(blockFactory, ElementType.fromJava(multiValue.get(0).getClass()), positions);
                for (int i = 0; i < positions; i++) {
                    wrapper.accept(multiValue);
                }
                return wrapper.builder().build();
            }
            return BlockUtils.constantBlock(blockFactory, value, positions);
        }
    }
}
