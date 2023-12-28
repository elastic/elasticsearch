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
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.esql.evaluator.EvalMapper.toEvaluator;

public class InsensitiveEqualsMapper extends ExpressionMapper<InsensitiveEquals> {

    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> ints =
        InsensitiveEqualsIntsEvaluator.Factory::new;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> longs =
        InsensitiveEqualsLongsEvaluator.Factory::new;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> doubles =
        InsensitiveEqualsDoublesEvaluator.Factory::new;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> keywords =
        InsensitiveEqualsKeywordsEvaluator.Factory::new;
    private final TriFunction<Source, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory, ExpressionEvaluator.Factory> bools =
        InsensitiveEqualsBoolsEvaluator.Factory::new;

    @Override
    public final ExpressionEvaluator.Factory map(InsensitiveEquals bc, Layout layout) {
        DataType leftType = bc.left().dataType();
        DataType rightType = bc.right().dataType();
        if (leftType.isNumeric()) {
            DataType type = EsqlDataTypeRegistry.INSTANCE.commonType(leftType, bc.right().dataType());
            if (type == DataTypes.INTEGER) {
                return castToEvaluator(bc, layout, DataTypes.INTEGER, ints);
            }
            if (type == DataTypes.LONG) {
                return castToEvaluator(bc, layout, DataTypes.LONG, longs);
            }
            if (type == DataTypes.DOUBLE) {
                return castToEvaluator(bc, layout, DataTypes.DOUBLE, doubles);
            }
            if (type == DataTypes.UNSIGNED_LONG) {
                // using the long comparators will work on UL as well
                return castToEvaluator(bc, layout, DataTypes.UNSIGNED_LONG, longs);
            }
        }
        var leftEval = toEvaluator(bc.left(), layout);
        var rightEval = toEvaluator(bc.right(), layout);
        if (leftType == DataTypes.KEYWORD || leftType == DataTypes.TEXT || leftType == DataTypes.IP || leftType == DataTypes.VERSION) {
            if (bc.right().foldable() && EsqlDataTypes.isString(rightType)) {
                Object rightVal = BytesRefs.toBytesRef(bc.right().fold());
                Automaton automaton = AutomatonQueries.toCaseInsensitiveString(
                    rightVal instanceof BytesRef br ? br.utf8ToString() : String.valueOf(rightVal)
                );
                return dvrCtx -> new InsensitiveEqualsKeywordsConstantEvaluator(
                    bc.source(),
                    leftEval.get(dvrCtx),
                    new ByteRunAutomaton(automaton),
                    dvrCtx
                );
            }
            return keywords.apply(bc.source(), leftEval, rightEval);
        }
        if (leftType == DataTypes.BOOLEAN) {
            return bools.apply(bc.source(), leftEval, rightEval);
        }
        if (leftType == DataTypes.DATETIME) {
            return longs.apply(bc.source(), leftEval, rightEval);
        }
        if (leftType == EsqlDataTypes.GEO_POINT) {
            return longs.apply(bc.source(), leftEval, rightEval);
        }
        // TODO: Perhaps neithger geo_point, not cartesian_point should support comparisons?
        if (leftType == EsqlDataTypes.CARTESIAN_POINT) {
            return longs.apply(bc.source(), leftEval, rightEval);
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
