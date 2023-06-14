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
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the maximum value.
 */
public class MvMax extends AbstractMultivalueFunction {
    public MvMax(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), EsqlDataTypes::isRepresentable, sourceText(), null, "representable");
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case BOOLEAN -> () -> new MvMaxBooleanEvaluator(fieldEval.get());
            case BYTES_REF -> () -> new MvMaxBytesRefEvaluator(fieldEval.get());
            case DOUBLE -> () -> new MvMaxDoubleEvaluator(fieldEval.get());
            case INT -> () -> new MvMaxIntEvaluator(fieldEval.get());
            case LONG -> () -> new MvMaxLongEvaluator(fieldEval.get());
            case NULL -> () -> EvalOperator.CONSTANT_NULL;
            default -> throw new UnsupportedOperationException("unsupported type [" + field().dataType() + "]");
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMax(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMax::new, field());
    }

    @MvEvaluator(extraName = "Boolean")
    static boolean process(boolean current, boolean v) {
        return current || v;
    }

    @MvEvaluator(extraName = "BytesRef")
    static void process(BytesRef current, BytesRef v) {
        if (v.compareTo(current) > 0) {
            current.bytes = v.bytes;
            current.offset = v.offset;
            current.length = v.length;
        }
    }

    @MvEvaluator(extraName = "Double")
    static double process(double current, double v) {
        return Math.max(current, v);
    }

    @MvEvaluator(extraName = "Int")
    static int process(int current, int v) {
        return Math.max(current, v);
    }

    @MvEvaluator(extraName = "Long")
    static long process(long current, long v) {
        return Math.max(current, v);
    }
}
