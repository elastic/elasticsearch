/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NullMisuseSuggestion;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;

public class Not extends UnaryScalarFunction
    implements
        EvaluatorMapper,
        Negatable<Expression>,
        TranslationAware,
        AnyNullIsNull,
        NullMisuseSuggestion {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Not", Not::new);

    public Not(Source source, Expression child) {
        super(source, child);
    }

    private Not(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Not> info() {
        return NodeInfo.create(this, Not::new, field());
    }

    @Override
    protected Not replaceChild(Expression newChild) {
        return new Not(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataType.BOOLEAN == field().dataType()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isBoolean(field(), sourceText(), DEFAULT);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new NotEvaluatorFactory(source(), toEvaluator.apply(field()));
    }

    @Evaluator
    static boolean process(boolean v) {
        return false == v;
    }

    @Override
    protected Expression canonicalize() {
        if (field() instanceof Negatable) {
            return ((Negatable) field()).negate().canonical();
        }
        return super.canonicalize();
    }

    @Override
    public Expression negate() {
        return field();
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    static Expression negate(Expression exp) {
        return exp instanceof Negatable ? ((Negatable) exp).negate() : new Not(exp.source(), exp);
    }

    @Override
    public String nullMisuseAlternative() {
        Expression kept = keptNonNullOperand();
        // Suggesting `<literal> IS NOT NULL` (e.g. `5 IS NOT NULL`) is never what the user meant.
        if (kept == null || kept instanceof Literal) {
            return null;
        }
        String text = kept.sourceText();
        return text.isEmpty() ? null : text + " IS NOT NULL";
    }

    /**
     * Operand that would remain after rewriting {@code NOT (x == NULL)} / {@code x NOT IN (NULL)}
     * as {@code x IS NOT NULL}, or {@code null} if this {@code NOT} is not that shape.
     */
    private Expression keptNonNullOperand() {
        if (field() instanceof Equals equals) {
            return keptNonNullOperand(equals.left(), equals.right());
        }
        if (field() instanceof InsensitiveEquals insensitiveEquals) {
            return keptNonNullOperand(insensitiveEquals.left(), insensitiveEquals.right());
        }
        if (field() instanceof In in && in.list().stream().allMatch(Expressions::isGuaranteedNull)) {
            return in.value();
        }
        return null;
    }

    private static Expression keptNonNullOperand(Expression left, Expression right) {
        if (Expressions.isGuaranteedNull(right)) {
            return left;
        }
        if (Expressions.isGuaranteedNull(left)) {
            return right;
        }
        return null;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return TranslationAware.translatable(field(), pushdownPredicates).negate();
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return handler.asQuery(pushdownPredicates, field()).negate(source());
    }

    record NotEvaluatorFactory(Source source, ExpressionEvaluator.Factory field) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new NotEvaluator(source, field.get(context), context);
        }

        @Override
        public String toString() {
            return "NotEvaluator[field=" + field + ']';
        }
    }
}
