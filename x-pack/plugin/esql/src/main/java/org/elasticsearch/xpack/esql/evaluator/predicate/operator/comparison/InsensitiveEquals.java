/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.TypeResolutions;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;

public class InsensitiveEquals extends InsensitiveBinaryComparison {

    public InsensitiveEquals(Source source, Expression left, Expression right) {
        super(source, left, right);
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
        return AutomatonQueries.toCaseInsensitiveString(val.utf8ToString());
    }

    @Override
    public Boolean fold() {
        BytesRef leftVal = BytesRefs.toBytesRef(left().fold());
        BytesRef rightVal = BytesRefs.toBytesRef(right().fold());
        if (leftVal == null || rightVal == null) {
            return null;
        }
        return process(leftVal, rightVal);
    }
}
