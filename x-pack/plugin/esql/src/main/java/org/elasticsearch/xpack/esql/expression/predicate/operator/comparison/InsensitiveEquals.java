/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.core.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;

public class InsensitiveEquals extends InsensitiveBinaryComparison implements Validatable {
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

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isString(left(), sourceText(), TypeResolutions.ParamOrdinal.FIRST)
            .and(TypeResolutions.isString(right(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
    }

    @Override
    public void validate(Failures failures) {
        failures.add(isFoldable(right(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
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
