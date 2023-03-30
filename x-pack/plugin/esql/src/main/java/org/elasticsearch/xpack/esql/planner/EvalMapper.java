/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.NotProcessor;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.List;
import java.util.function.Supplier;

public final class EvalMapper {
    abstract static class ExpressionMapper<E extends Expression> {
        private final Class<E> typeToken;

        protected ExpressionMapper() {
            typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());
        }

        protected abstract Supplier<ExpressionEvaluator> map(E expression, Layout layout);
    }

    private static final List<ExpressionMapper<?>> MAPPERS = List.of(
        ArithmeticMapper.ADD,
        ArithmeticMapper.DIV,
        ArithmeticMapper.MOD,
        ArithmeticMapper.MUL,
        ArithmeticMapper.SUB,
        ComparisonMapper.EQUALS,
        ComparisonMapper.NOT_EQUALS,
        ComparisonMapper.GREATER_THAN,
        ComparisonMapper.GREATER_THAN_OR_EQUAL,
        ComparisonMapper.LESS_THAN,
        ComparisonMapper.LESS_THAN_OR_EQUAL,
        new BooleanLogic(),
        new Nots(),
        new Attributes(),
        new Literals()
    );

    private EvalMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Supplier<ExpressionEvaluator> toEvaluator(Expression exp, Layout layout) {
        if (exp instanceof Mappable m) {
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
        protected Supplier<ExpressionEvaluator> map(BinaryLogic bc, Layout layout) {
            Supplier<ExpressionEvaluator> leftEval = toEvaluator(bc.left(), layout);
            Supplier<ExpressionEvaluator> rightEval = toEvaluator(bc.right(), layout);
            record BooleanLogicExpressionEvaluator(BinaryLogic bl, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return bl.function().apply((Boolean) leftEval.computeRow(page, pos), (Boolean) rightEval.computeRow(page, pos));
                }
            }
            return () -> new BooleanLogicExpressionEvaluator(bc, leftEval.get(), rightEval.get());
        }
    }

    static class Nots extends ExpressionMapper<Not> {

        @Override
        protected Supplier<ExpressionEvaluator> map(Not not, Layout layout) {
            Supplier<ExpressionEvaluator> expEval = toEvaluator(not.field(), layout);
            record NotsExpressionEvaluator(ExpressionEvaluator expEval) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return NotProcessor.apply(expEval.computeRow(page, pos));
                }
            }
            return () -> new NotsExpressionEvaluator(expEval.get());
        }
    }

    static class Attributes extends ExpressionMapper<Attribute> {
        @Override
        protected Supplier<ExpressionEvaluator> map(Attribute attr, Layout layout) {
            // TODO these aren't efficient so we should do our best to remove them, but, for now, they are what we have
            int channel = layout.getChannel(attr.id());
            if (attr.dataType() == DataTypes.DOUBLE) {
                record Doubles(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        DoubleBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getDouble(block.getFirstValueIndex(pos));
                    }
                }
                return () -> new Doubles(channel);
            }
            if (attr.dataType() == DataTypes.LONG || attr.dataType() == DataTypes.DATETIME) {
                record Longs(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        LongBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getLong(block.getFirstValueIndex(pos));
                    }
                }
                return () -> new Longs(channel);
            }
            if (attr.dataType() == DataTypes.INTEGER) {
                record Ints(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        IntBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getInt(block.getFirstValueIndex(pos));
                    }
                }
                return () -> new Ints(channel);
            }
            if (attr.dataType() == DataTypes.KEYWORD) {
                record Keywords(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        BytesRefBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getBytesRef(block.getFirstValueIndex(pos), new BytesRef());
                    }
                }
                return () -> new Keywords(channel);
            }
            if (attr.dataType() == DataTypes.BOOLEAN) {
                record Booleans(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        BooleanBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getBoolean(block.getFirstValueIndex(pos));
                    }
                }
                return () -> new Booleans(channel);
            }
            throw new UnsupportedOperationException("unsupported field type [" + attr.dataType().typeName() + "]");
        }
    }

    static class Literals extends ExpressionMapper<Literal> {

        @Override
        protected Supplier<ExpressionEvaluator> map(Literal lit, Layout layout) {
            record LiteralsExpressionEvaluator(Literal lit) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return lit.value();
                }
            }

            assert checkDataType(lit) : "unsupported data value [" + lit.value() + "] for data type [" + lit.dataType() + "]";
            return () -> new LiteralsExpressionEvaluator(lit);
        }

        private boolean checkDataType(Literal lit) {
            if (lit.value() == null) {
                // Null is always ok
                return true;
            }
            return switch (LocalExecutionPlanner.toElementType(lit.dataType())) {
                case BOOLEAN -> lit.value() instanceof Boolean;
                case BYTES_REF -> lit.value() instanceof BytesRef;
                case DOUBLE -> lit.value() instanceof Double;
                case INT -> lit.value() instanceof Integer;
                case LONG -> lit.value() instanceof Long;
                case NULL -> true;
                case DOC, UNKNOWN -> false;
            };
        }
    }
}
