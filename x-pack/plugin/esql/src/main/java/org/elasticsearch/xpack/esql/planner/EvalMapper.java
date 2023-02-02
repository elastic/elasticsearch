/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.NotProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.Arrays;
import java.util.List;

final class EvalMapper {

    abstract static class ExpressionMapper<E extends Expression> {
        private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        protected abstract ExpressionEvaluator map(E expression, Layout layout);
    }

    private static final List<ExpressionMapper<?>> MAPPERS = Arrays.asList(
        new Arithmetics(),
        new Comparisons(),
        new BooleanLogic(),
        new Nots(),
        new Attributes(),
        new Literals(),
        new RoundFunction(),
        new LengthFunction(),
        new DateFormatFunction()
    );

    private EvalMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static ExpressionEvaluator toEvaluator(Expression exp, Layout layout) {
        for (ExpressionMapper em : MAPPERS) {
            if (em.typeToken.isInstance(exp)) {
                return em.map(exp, layout);
            }
        }
        throw new QlIllegalArgumentException("Unsupported expression [{}]", exp);
    }

    static class Arithmetics extends ExpressionMapper<ArithmeticOperation> {

        @Override
        protected ExpressionEvaluator map(ArithmeticOperation ao, Layout layout) {
            ExpressionEvaluator leftEval = toEvaluator(ao.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(ao.right(), layout);
            record ArithmeticExpressionEvaluator(ArithmeticOperation ao, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return ao.function().apply(leftEval.computeRow(page, pos), rightEval.computeRow(page, pos));
                }
            }
            return new ArithmeticExpressionEvaluator(ao, leftEval, rightEval);
        }

    }

    static class Comparisons extends ExpressionMapper<BinaryComparison> {

        @Override
        protected ExpressionEvaluator map(BinaryComparison bc, Layout layout) {
            ExpressionEvaluator leftEval = toEvaluator(bc.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(bc.right(), layout);
            record ComparisonsExpressionEvaluator(BinaryComparison bc, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return bc.function().apply(leftEval.computeRow(page, pos), rightEval.computeRow(page, pos));
                }
            }
            return new ComparisonsExpressionEvaluator(bc, leftEval, rightEval);
        }
    }

    static class BooleanLogic extends ExpressionMapper<BinaryLogic> {

        @Override
        protected ExpressionEvaluator map(BinaryLogic bc, Layout layout) {
            ExpressionEvaluator leftEval = toEvaluator(bc.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(bc.right(), layout);
            record BooleanLogicExpressionEvaluator(BinaryLogic bl, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
                implements
                    ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return bl.function().apply((Boolean) leftEval.computeRow(page, pos), (Boolean) rightEval.computeRow(page, pos));
                }
            }
            return new BooleanLogicExpressionEvaluator(bc, leftEval, rightEval);
        }
    }

    static class Nots extends ExpressionMapper<Not> {

        @Override
        protected ExpressionEvaluator map(Not not, Layout layout) {
            ExpressionEvaluator expEval = toEvaluator(not.field(), layout);
            record NotsExpressionEvaluator(ExpressionEvaluator expEval) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return NotProcessor.apply(expEval.computeRow(page, pos));
                }
            }
            return new NotsExpressionEvaluator(expEval);
        }
    }

    static class Attributes extends ExpressionMapper<Attribute> {
        @Override
        protected ExpressionEvaluator map(Attribute attr, Layout layout) {
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
                        return block.getDouble(pos);
                    }
                }
                return new Doubles(channel);
            }
            if (attr.dataType() == DataTypes.LONG || attr.dataType() == DataTypes.DATETIME) {
                record Longs(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        LongBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getLong(pos);
                    }
                }
                return new Longs(channel);
            }
            if (attr.dataType() == DataTypes.INTEGER) {
                record Ints(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        Block b = page.getBlock(channel);
                        if (b.elementType() == ElementType.LONG) {
                            // TODO hack for allowing eval after stats which doesn't respect the int type
                            LongBlock hack = (LongBlock) b;
                            if (hack.isNull(pos)) {
                                return null;
                            }
                            return hack.getLong(pos);
                        }
                        IntBlock block = (IntBlock) b;
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getInt(pos);
                    }
                }
                return new Ints(channel);
            }
            if (attr.dataType() == DataTypes.KEYWORD) {
                record Keywords(int channel) implements ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        BytesRefBlock block = page.getBlock(channel);
                        if (block.isNull(pos)) {
                            return null;
                        }
                        return block.getBytesRef(pos, new BytesRef());
                    }
                }
                return new Keywords(channel);
            }
            throw new UnsupportedOperationException("unsupported field type [" + attr.dataType() + "]");
        }
    }

    static class Literals extends ExpressionMapper<Literal> {

        @Override
        protected ExpressionEvaluator map(Literal lit, Layout layout) {
            record LiteralsExpressionEvaluator(Literal lit) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return lit.value();
                }
            }
            return new LiteralsExpressionEvaluator(lit);
        }
    }

    static class RoundFunction extends ExpressionMapper<Round> {

        @Override
        protected ExpressionEvaluator map(Round round, Layout layout) {
            ExpressionEvaluator fieldEvaluator = toEvaluator(round.field(), layout);
            // round.decimals() == null means that decimals were not provided (it's an optional parameter of the Round function)
            ExpressionEvaluator decimalsEvaluator = round.decimals() != null ? toEvaluator(round.decimals(), layout) : null;
            if (round.field().dataType().isRational()) {
                record DecimalRoundExpressionEvaluator(ExpressionEvaluator fieldEvaluator, ExpressionEvaluator decimalsEvaluator)
                    implements
                        ExpressionEvaluator {
                    @Override
                    public Object computeRow(Page page, int pos) {
                        // decimals could be null
                        // it's not the same null as round.decimals() being null
                        Object decimals = decimalsEvaluator != null ? decimalsEvaluator.computeRow(page, pos) : null;
                        return Round.process(fieldEvaluator.computeRow(page, pos), decimals);
                    }
                }
                return new DecimalRoundExpressionEvaluator(fieldEvaluator, decimalsEvaluator);
            } else {
                return fieldEvaluator;
            }
        }
    }

    static class LengthFunction extends ExpressionMapper<Length> {

        @Override
        protected ExpressionEvaluator map(Length length, Layout layout) {
            record LengthFunctionExpressionEvaluator(ExpressionEvaluator exp) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return Length.process(((BytesRef) exp.computeRow(page, pos)).utf8ToString());
                }
            }
            return new LengthFunctionExpressionEvaluator(toEvaluator(length.field(), layout));
        }
    }

    public static class DateFormatFunction extends ExpressionMapper<DateFormat> {
        @Override
        public ExpressionEvaluator map(DateFormat df, Layout layout) {
            record DateFormatEvaluator(ExpressionEvaluator exp, ExpressionEvaluator formatEvaluator) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    Object format = formatEvaluator != null ? formatEvaluator.computeRow(page, pos) : null;
                    return DateFormat.process(((Long) exp.computeRow(page, pos)), toFormatter(format));
                }
            }

            record ConstantDateFormatEvaluator(ExpressionEvaluator exp, DateFormatter formatter) implements ExpressionEvaluator {
                @Override
                public Object computeRow(Page page, int pos) {
                    return DateFormat.process(((Long) exp.computeRow(page, pos)), formatter);
                }
            }

            ExpressionEvaluator fieldEvaluator = toEvaluator(df.field(), layout);
            Expression format = df.format();
            if (format == null || format.foldable()) {
                return new ConstantDateFormatEvaluator(fieldEvaluator, toFormatter(format == null ? null : format.fold()));
            }
            return new DateFormatEvaluator(fieldEvaluator, toEvaluator(format, layout));
        }

        private static DateFormatter toFormatter(Object format) {
            return format == null ? DateFormat.DEFAULT_DATE_FORMATTER : DateFormatter.forPattern(format.toString());
        }
    }
}
