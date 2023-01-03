/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
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
        new LengthFunction()
    );

    private EvalMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static ExpressionEvaluator toEvaluator(Expression exp, Layout layout) {
        ExpressionMapper mapper = null;
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
            return (page, pos) -> ao.function().apply(leftEval.computeRow(page, pos), rightEval.computeRow(page, pos));
        }
    }

    static class Comparisons extends ExpressionMapper<BinaryComparison> {

        @Override
        protected ExpressionEvaluator map(BinaryComparison bc, Layout layout) {
            ExpressionEvaluator leftEval = toEvaluator(bc.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(bc.right(), layout);
            return (page, pos) -> bc.function().apply(leftEval.computeRow(page, pos), rightEval.computeRow(page, pos));
        }
    }

    static class BooleanLogic extends ExpressionMapper<BinaryLogic> {

        @Override
        protected ExpressionEvaluator map(BinaryLogic bc, Layout layout) {
            ExpressionEvaluator leftEval = toEvaluator(bc.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(bc.right(), layout);
            return (page, pos) -> bc.function().apply((Boolean) leftEval.computeRow(page, pos), (Boolean) rightEval.computeRow(page, pos));
        }
    }

    static class Nots extends ExpressionMapper<Not> {

        @Override
        protected ExpressionEvaluator map(Not not, Layout layout) {
            ExpressionEvaluator expEval = toEvaluator(not.field(), layout);
            return (page, pos) -> NotProcessor.apply(expEval.computeRow(page, pos));
        }
    }

    static class Attributes extends ExpressionMapper<Attribute> {
        @Override
        protected ExpressionEvaluator map(Attribute attr, Layout layout) {
            int channel = layout.getChannel(attr.id());
            return (page, pos) -> page.getBlock(channel).getObject(pos);
        }
    }

    static class Literals extends ExpressionMapper<Literal> {

        @Override
        protected ExpressionEvaluator map(Literal lit, Layout layout) {
            return (page, pos) -> lit.value();
        }
    }

    static class RoundFunction extends ExpressionMapper<Round> {

        @Override
        protected ExpressionEvaluator map(Round round, Layout layout) {
            ExpressionEvaluator fieldEvaluator = toEvaluator(round.field(), layout);
            // round.decimals() == null means that decimals were not provided (it's an optional parameter of the Round function)
            ExpressionEvaluator decimalsEvaluator = round.decimals() != null ? toEvaluator(round.decimals(), layout) : null;
            if (round.field().dataType().isRational()) {
                return (page, pos) -> {
                    // decimals could be null
                    // it's not the same null as round.decimals() being null
                    Object decimals = decimalsEvaluator != null ? decimalsEvaluator.computeRow(page, pos) : null;
                    return Round.process(fieldEvaluator.computeRow(page, pos), decimals);
                };
            } else {
                return fieldEvaluator;
            }
        }
    }

    static class LengthFunction extends ExpressionMapper<Length> {

        @Override
        protected ExpressionEvaluator map(Length length, Layout layout) {
            ExpressionEvaluator e1 = toEvaluator(length.field(), layout);
            return (page, pos) -> Length.process(((BytesRef) e1.computeRow(page, pos)).utf8ToString());
        }
    }
}
