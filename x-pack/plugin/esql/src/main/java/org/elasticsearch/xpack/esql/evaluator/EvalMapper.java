/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.ComparisonMapper;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InMapper;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RegexMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;

import java.util.List;
import java.util.function.IntFunction;

public final class EvalMapper {

    private static final List<ExpressionMapper<?>> MAPPERS = List.of(
        ComparisonMapper.EQUALS,
        ComparisonMapper.NOT_EQUALS,
        ComparisonMapper.GREATER_THAN,
        ComparisonMapper.GREATER_THAN_OR_EQUAL,
        ComparisonMapper.LESS_THAN,
        ComparisonMapper.LESS_THAN_OR_EQUAL,
        InMapper.IN_MAPPER,
        RegexMapper.REGEX_MATCH,
        new BooleanLogic(),
        new Nots(),
        new Attributes(),
        new Literals(),
        new IsNotNulls(),
        new IsNulls()
    );

    private EvalMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static ExpressionEvaluator.Factory toEvaluator(Expression exp, Layout layout) {
        if (exp instanceof EvaluatorMapper m) {
            return m.toEvaluator(e -> toEvaluator(e, layout));
        }
        for (ExpressionMapper em : MAPPERS) {
            if (em.typeToken.isInstance(exp)) {
                return em.map(exp, layout);
            }
        }
        throw new QlIllegalArgumentException("Unsupported expression [{}]", exp);
    }

    static class BooleanLogic extends ExpressionMapper<BinaryLogic> {
        @Override
        public ExpressionEvaluator.Factory map(BinaryLogic bc, Layout layout) {
            var leftEval = toEvaluator(bc.left(), layout);
            var rightEval = toEvaluator(bc.right(), layout);
            /**
             * Evaluator for the <href a="https://en.wikipedia.org/wiki/Three-valued_logic">three-valued boolean expressions</href>.
             * We can't generate these with the {@link Evaluator} annotation because that
             * always implements viral null. And three-valued boolean expressions don't.
             * {@code false AND null} is {@code false} and {@code true OR null} is {@code true}.
             */
            record BooleanLogicExpressionEvaluator(BinaryLogic bl, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {
                @Override
                public Block.Ref eval(Page page) {
                    try (Block.Ref lhs = leftEval.eval(page); Block.Ref rhs = rightEval.eval(page)) {
                        Vector lhsVector = lhs.block().asVector();
                        Vector rhsVector = rhs.block().asVector();
                        if (lhsVector != null && rhsVector != null) {
                            return Block.Ref.floating(eval((BooleanVector) lhsVector, (BooleanVector) rhsVector));
                        }
                        return Block.Ref.floating(eval(lhs.block(), rhs.block()));
                    }
                }

                /**
                 * Eval blocks, handling {@code null}. This takes {@link Block} instead of
                 * {@link BooleanBlock} because blocks that <strong>only</strong> contain
                 * {@code null} can't be cast to {@link BooleanBlock}. So we check for
                 * {@code null} first and don't cast at all if the value is {@code null}.
                 */
                private Block eval(Block lhs, Block rhs) {
                    int positionCount = lhs.getPositionCount();
                    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
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

                private Block eval(BooleanVector lhs, BooleanVector rhs) {
                    int positionCount = lhs.getPositionCount();
                    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
                    for (int p = 0; p < positionCount; p++) {
                        result.appendBoolean(bl.function().apply(lhs.getBoolean(p), rhs.getBoolean(p)));
                    }
                    return result.build().asBlock();
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(leftEval, rightEval);
                }
            }
            return driverContext -> new BooleanLogicExpressionEvaluator(bc, leftEval.get(driverContext), rightEval.get(driverContext));
        }
    }

    static class Nots extends ExpressionMapper<Not> {
        @Override
        public ExpressionEvaluator.Factory map(Not not, Layout layout) {
            var expEval = toEvaluator(not.field(), layout);
            return dvrCtx -> new org.elasticsearch.xpack.esql.evaluator.predicate.operator.logical.NotEvaluator(
                expEval.get(dvrCtx),
                dvrCtx
            );
        }
    }

    static class Attributes extends ExpressionMapper<Attribute> {
        @Override
        public ExpressionEvaluator.Factory map(Attribute attr, Layout layout) {
            record Attribute(int channel) implements ExpressionEvaluator {
                @Override
                public Block.Ref eval(Page page) {
                    return new Block.Ref(page.getBlock(channel), page);
                }

                @Override
                public void close() {}
            }
            int channel = layout.get(attr.id()).channel();
            return driverContext -> new Attribute(channel);
        }
    }

    static class Literals extends ExpressionMapper<Literal> {

        @Override
        public ExpressionEvaluator.Factory map(Literal lit, Layout layout) {
            record LiteralsEvaluator(IntFunction<Block> block) implements ExpressionEvaluator {
                @Override
                public Block.Ref eval(Page page) {
                    return Block.Ref.floating(block.apply(page.getPositionCount()));
                }

                @Override
                public void close() {}
            }
            // wrap the closure to provide a nice toString (used by tests)
            var blockClosure = new IntFunction<Block>() {
                @Override
                public Block apply(int value) {
                    return block(lit).apply(value);
                }

                @Override
                public String toString() {
                    return lit.toString();
                }
            };
            return driverContext -> new LiteralsEvaluator(blockClosure);
        }

        private IntFunction<Block> block(Literal lit) {
            var value = lit.value();
            if (value == null) {
                return Block::constantNullBlock;
            }

            if (value instanceof List<?> multiValue) {
                if (multiValue.isEmpty()) {
                    return Block::constantNullBlock;
                }
                return positions -> {
                    var wrapper = BlockUtils.wrapperFor(ElementType.fromJava(multiValue.get(0).getClass()), positions);
                    wrapper.accept(multiValue);
                    return wrapper.builder().build();
                };
            }
            return positions -> BlockUtils.constantBlock(value, positions);
        }
    }

    static class IsNulls extends ExpressionMapper<IsNull> {

        @Override
        public ExpressionEvaluator.Factory map(IsNull isNull, Layout layout) {
            var field = toEvaluator(isNull.field(), layout);
            return driverContext -> new IsNullEvaluator(field.get(driverContext));
        }

        record IsNullEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
            @Override
            public Block.Ref eval(Page page) {
                try (Block.Ref fieldBlock = field.eval(page)) {
                    if (fieldBlock.block().asVector() != null) {
                        return Block.Ref.floating(BooleanBlock.newConstantBlockWith(false, page.getPositionCount()));
                    }
                    boolean[] result = new boolean[page.getPositionCount()];
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        result[p] = fieldBlock.block().isNull(p);
                    }
                    return Block.Ref.floating(new BooleanArrayVector(result, result.length).asBlock());
                }
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(field);
            }
        }
    }

    static class IsNotNulls extends ExpressionMapper<IsNotNull> {

        @Override
        public ExpressionEvaluator.Factory map(IsNotNull isNotNull, Layout layout) {
            var field = toEvaluator(isNotNull.field(), layout);
            return driverContext -> new IsNotNullEvaluator(field.get(driverContext));
        }

        record IsNotNullEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
            @Override
            public Block.Ref eval(Page page) {
                try (Block.Ref fieldBlock = field.eval(page)) {
                    if (fieldBlock.block().asVector() != null) {
                        return Block.Ref.floating(BooleanBlock.newConstantBlockWith(true, page.getPositionCount()));
                    }
                    boolean[] result = new boolean[page.getPositionCount()];
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        result[p] = fieldBlock.block().isNull(p) == false;
                    }
                    return Block.Ref.floating(new BooleanArrayVector(result, result.length).asBlock());
                }
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(field);
            }
        }
    }
}
