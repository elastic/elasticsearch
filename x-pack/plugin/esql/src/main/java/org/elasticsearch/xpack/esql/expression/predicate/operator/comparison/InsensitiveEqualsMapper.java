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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.List;

import static org.elasticsearch.xpack.esql.evaluator.EvalMapper.toEvaluator;

public class InsensitiveEqualsMapper extends ExpressionMapper<InsensitiveEquals> {

    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords =
        InsensitiveEqualsEvaluator.Factory::new;

    @Override
    public final ExpressionEvaluator.Factory map(
        FoldContext foldCtx,
        InsensitiveEquals bc,
        Layout layout,
        List<ShardContext> shardContexts
    ) {
        DataType leftType = bc.left().dataType();
        DataType rightType = bc.right().dataType();

        var leftEval = toEvaluator(foldCtx, bc.left(), layout, shardContexts);
        var rightEval = toEvaluator(foldCtx, bc.right(), layout, shardContexts);
        if (DataType.isString(leftType)) {
            if (bc.right().foldable() && DataType.isString(rightType)) {
                BytesRef rightVal = BytesRefs.toBytesRef(bc.right().fold(FoldContext.small() /* TODO remove me */));
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
}
