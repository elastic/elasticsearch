/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatial;

/**
 * Reduce a multivalued field to a single valued field containing the maximum value.
 */
public class MvMax extends AbstractMultivalueFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvMax", MvMax::new);

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
        description = "Converts a multivalued expression into a single valued column containing the maximum value.",
        examples = {
            @Example(file = "math", tag = "mv_max"),
            @Example(
                description = "It can be used by any column type, including `keyword` columns. "
                    + "In that case it picks the last string, comparing their utf-8 representation byte by byte:",
                file = "string",
                tag = "mv_max"
            ) }
    )
    public MvMax(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Multivalue expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private MvMax(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> isSpatial(t) == false && isRepresentable(t), sourceText(), null, "representableNonSpatial");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (PlannerUtils.toSortableElementType(field().dataType())) {
            case BOOLEAN -> new MvMaxBooleanEvaluator.Factory(fieldEval);
            case BYTES_REF -> new MvMaxBytesRefEvaluator.Factory(fieldEval);
            case DOUBLE -> new MvMaxDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvMaxIntEvaluator.Factory(fieldEval);
            case LONG -> new MvMaxLongEvaluator.Factory(fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
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

    @MvEvaluator(extraName = "Boolean", ascending = "ascendingIndex")
    static boolean process(boolean current, boolean v) {
        return current || v;
    }

    @MvEvaluator(extraName = "BytesRef", ascending = "ascendingIndex")
    static void process(BytesRef current, BytesRef v) {
        if (v.compareTo(current) > 0) {
            current.bytes = v.bytes;
            current.offset = v.offset;
            current.length = v.length;
        }
    }

    @MvEvaluator(extraName = "Double", ascending = "ascendingIndex")
    static double process(double current, double v) {
        return Math.max(current, v);
    }

    @MvEvaluator(extraName = "Int", ascending = "ascendingIndex")
    static int process(int current, int v) {
        return Math.max(current, v);
    }

    @MvEvaluator(extraName = "Long", ascending = "ascendingIndex")
    static long process(long current, long v) {
        return Math.max(current, v);
    }

    /**
     * If the values as ascending pick the final value.
     */
    static int ascendingIndex(int count) {
        return count - 1;
    }
}
