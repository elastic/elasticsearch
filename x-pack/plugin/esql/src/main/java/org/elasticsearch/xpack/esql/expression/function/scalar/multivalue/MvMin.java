/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the minimum value.
 */
public class MvMin extends AbstractMultivalueFunction {
    @FunctionInfo(returnType = "?", description = "Reduce a multivalued field to a single valued field containing the minimum value.")
    public MvMin(
        Source source,
        @Param(
            name = "v",
            type = { "unsigned_long", "date", "boolean", "double", "ip", "text", "integer", "keyword", "version", "long" }
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), EsqlDataTypes::isRepresentable, sourceText(), null, "representable");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case BOOLEAN -> new MvMinBooleanEvaluator.Factory(fieldEval);
            case BYTES_REF -> new MvMinBytesRefEvaluator.Factory(fieldEval);
            case DOUBLE -> new MvMinDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvMinIntEvaluator.Factory(fieldEval);
            case LONG -> new MvMinLongEvaluator.Factory(fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMin(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMin::new, field());
    }

    @MvEvaluator(extraName = "Boolean", ascending = "ascendingIndex")
    static boolean process(boolean current, boolean v) {
        return current && v;
    }

    @MvEvaluator(extraName = "BytesRef", ascending = "ascendingIndex")
    static void process(BytesRef current, BytesRef v) {
        if (v.compareTo(current) < 0) {
            current.bytes = v.bytes;
            current.offset = v.offset;
            current.length = v.length;
        }
    }

    @MvEvaluator(extraName = "Double", ascending = "ascendingIndex")
    static double process(double current, double v) {
        return Math.min(current, v);
    }

    @MvEvaluator(extraName = "Int", ascending = "ascendingIndex")
    static int process(int current, int v) {
        return Math.min(current, v);
    }

    @MvEvaluator(extraName = "Long", ascending = "ascendingIndex")
    static long process(long current, long v) {
        return Math.min(current, v);
    }

    /**
     * If the values are ascending pick the first value.
     */
    static int ascendingIndex(int count) {
        return 0;
    }
}
