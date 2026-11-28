/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.compute.expression.DictionaryPredicateEvaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.Collection;

/**
 * Wraps warning-free single-field KEYWORD/TEXT boolean predicates in
 * {@link DictionaryPredicateEvaluator} so dictionary-encoded pages evaluate once
 * per distinct referenced ordinal.
 * <p>
 * {@link EvalMapper#toEvaluatorContext(Layout, EvaluatorMapper.ToEvaluator)} skips this
 * hook so the wrapper is not applied twice on the inner tree.
 */
public final class DictionaryEvaluation {
    private DictionaryEvaluation() {}

    static ExpressionEvaluator.Factory maybeWrap(Expression exp, Layout layout, EvaluatorMapper.ToEvaluator parent) {
        if (exp.dataType() != DataType.BOOLEAN || exp.foldable() || exp instanceof EvaluatorMapper == false) {
            return null;
        }
        var refs = exp.references();
        if (refs.size() != 1) {
            return null;
        }
        Attribute attr = refs.iterator().next();
        if (isKeywordOrText(attr.dataType()) == false) {
            return null;
        }
        Layout.ChannelAndType channel = layout.get(attr.id());
        if (channel == null) {
            return null;
        }
        if (isAllowlisted(exp, true) == false) {
            return null;
        }
        // Two trees: original layout (per-row fallback) and a 1-channel dict layout. Double
        // automaton construction for large RLIKE/LIKE lists is acceptable; wrapping is rare.
        ExpressionEvaluator.Factory perRow = ((EvaluatorMapper) exp).toEvaluator(EvalMapper.toEvaluatorContext(layout, parent));
        Layout dictLayout = new Layout.Builder().append(attr).build();
        ExpressionEvaluator.Factory dictionary = ((EvaluatorMapper) exp).toEvaluator(EvalMapper.toEvaluatorContext(dictLayout, parent));
        return new DictionaryPredicateEvaluator.Factory(perRow, dictionary, channel.channel());
    }

    static boolean isAllowlisted(Expression exp, boolean root) {
        if (exp instanceof Literal lit) {
            return (lit.value() instanceof Collection) == false;
        }
        if (exp instanceof Attribute) {
            return true;
        }
        if (exp instanceof Equals || exp instanceof NotEquals) {
            return isKeywordOrText(exp.children().get(0).dataType())
                && isKeywordOrText(exp.children().get(1).dataType())
                && childrenAllowlisted(exp);
        }
        if (exp instanceof In in) {
            return isKeywordOrText(in.value().dataType()) && childrenAllowlisted(exp);
        }
        if (exp instanceof StartsWith
            || exp instanceof EndsWith
            || exp instanceof WildcardLike
            || exp instanceof RLike
            || exp instanceof WildcardLikeList
            || exp instanceof RLikeList
            || exp instanceof InsensitiveEquals) {
            return childrenAllowlisted(exp);
        }
        if (exp instanceof ToLower || exp instanceof ToUpper) {
            return childrenAllowlisted(exp);
        }
        if (exp instanceof IsNull || exp instanceof IsNotNull) {
            return root == false && childrenAllowlisted(exp);
        }
        if (exp instanceof Not || exp instanceof And || exp instanceof Or) {
            return childrenAllowlisted(exp);
        }
        return false;
    }

    private static boolean childrenAllowlisted(Expression exp) {
        for (Expression child : exp.children()) {
            if (isAllowlisted(child, false) == false) {
                return false;
            }
        }
        return true;
    }

    static boolean isKeywordOrText(DataType dataType) {
        return dataType == DataType.KEYWORD || dataType == DataType.TEXT;
    }
}
