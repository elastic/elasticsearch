/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;

public class InsensitiveEquals extends InsensitiveBinaryComparison {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "InsensitiveEquals",
        InsensitiveEquals::new
    );

    public InsensitiveEquals(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    private InsensitiveEquals(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<InsensitiveEquals> info() {
        return NodeInfo.create(this, InsensitiveEquals::new, left(), right());
    }

    @Override
    protected InsensitiveEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new InsensitiveEquals(source(), newLeft, newRight);
    }

    @Evaluator
    static boolean process(BytesRef lhs, BytesRef rhs) {
        return processConstant(lhs, new ByteRunAutomaton(automaton(rhs)));
    }

    @Evaluator(extraName = "Constant")
    static boolean processConstant(BytesRef lhs, @Fixed ByteRunAutomaton rhs) {
        return rhs.run(lhs.bytes, lhs.offset, lhs.length);
    }

    public String symbol() {
        return "=~";
    }

    protected TypeResolution resolveType() {
        return TypeResolutions.isString(left(), sourceText(), TypeResolutions.ParamOrdinal.FIRST)
            .and(TypeResolutions.isString(right(), sourceText(), TypeResolutions.ParamOrdinal.SECOND))
            .and(TypeResolutions.isFoldable(right(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
    }

    public static Automaton automaton(BytesRef val) {
        if (val.length == 0) {
            // toCaseInsensitiveString doesn't match empty strings properly so let's do it ourselves
            return Automata.makeEmptyString();
        }
        return AutomatonQueries.toCaseInsensitiveString(val.utf8ToString());
    }

    @Override
    public Boolean fold(FoldContext ctx) {
        BytesRef leftVal = BytesRefs.toBytesRef(left().fold(ctx));
        BytesRef rightVal = BytesRefs.toBytesRef(right().fold(ctx));
        if (leftVal == null || rightVal == null) {
            return null;
        }
        return process(leftVal, rightVal);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableFieldAttribute(left()) && right().foldable() ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        checkInsensitiveComparison();
        return translate();
    }

    private void checkInsensitiveComparison() {
        Check.isTrue(
            right().foldable(),
            "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
            right().sourceLocation().getLineNumber(),
            right().sourceLocation().getColumnNumber(),
            Expressions.name(right()),
            symbol()
        );
    }

    private Query translate() {
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(left());
        BytesRef value = BytesRefs.toBytesRef(valueOf(FoldContext.small() /* TODO remove me */, right()));
        String name = LucenePushdownPredicates.pushableAttributeName(attribute);
        return new TermQuery(source(), name, value.utf8ToString(), true);
    }

    @Override
    public Expression singleValueField() {
        return left();
    }
}
