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
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.planner.Layout;

import static org.elasticsearch.xpack.esql.evaluator.EvalMapper.toEvaluator;

public class InsensitiveEqualsMapper extends ExpressionMapper<InsensitiveEquals> {

    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords =
        InsensitiveEqualsEvaluator.Factory::new;

    @Override
    public final ExpressionEvaluator.Factory map(InsensitiveEquals bc, Layout layout) {
        DataType leftType = bc.left().dataType();
        DataType rightType = bc.right().dataType();

        var leftEval = toEvaluator(bc.left(), layout);
        var rightEval = toEvaluator(bc.right(), layout);
        if (leftType == DataType.KEYWORD || leftType == DataType.TEXT) {
            if (bc.right().foldable() && DataType.isString(rightType)) {
                BytesRef rightVal = BytesRefs.toBytesRef(bc.right().fold());
                Automaton automaton = InsensitiveEquals.automaton(rightVal);
                return dvrCtx -> new InsensitiveEqualsConstantEvaluator(
                    bc.source(),
                    leftEval.get(dvrCtx),
                    new ByteRunAutomaton(automaton),
                    dvrCtx
                );
            }
            return keywords.apply(bc.source(), leftEval, rightEval);
        }
        throw new EsqlIllegalArgumentException("resolved type for [" + bc + "] but didn't implement mapping");
    }

    public static ExpressionEvaluator.Factory castToEvaluator(
        InsensitiveEquals op,
        Layout layout,
        DataType required,
        TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> factory
    ) {
        var lhs = Cast.cast(op.source(), op.left().dataType(), required, toEvaluator(op.left(), layout));
        var rhs = Cast.cast(op.source(), op.right().dataType(), required, toEvaluator(op.right(), layout));
        return factory.apply(op.source(), lhs, rhs);
    }
}
