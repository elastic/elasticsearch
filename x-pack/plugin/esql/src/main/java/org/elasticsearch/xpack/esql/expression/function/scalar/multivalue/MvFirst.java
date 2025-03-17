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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the minimum value.
 */
public class MvFirst extends AbstractMultivalueFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvFirst", MvFirst::new);

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        description = """
            Converts a multivalued expression into a single valued column containing the
            first value. This is most useful when reading from a function that emits
            multivalued columns in a known order like <<esql-split>>.""",
        detailedDescription = """
            The order that <<esql-multivalued-fields, multivalued fields>> are read from
            underlying storage is not guaranteed. It is **frequently** ascending, but don’t
            rely on that. If you need the minimum value use <<esql-mv_min>> instead of
            `MV_FIRST`. `MV_MIN` has optimizations for sorted values so there isn’t a
            performance benefit to `MV_FIRST`.""",
        examples = @Example(file = "string", tag = "mv_first")
    )
    public MvFirst(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Multivalue expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private MvFirst(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), DataType::isRepresentable, sourceText(), null, "representable");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case BOOLEAN -> new MvFirstBooleanEvaluator.Factory(fieldEval);
            case BYTES_REF -> new MvFirstBytesRefEvaluator.Factory(fieldEval);
            case DOUBLE -> new MvFirstDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvFirstIntEvaluator.Factory(fieldEval);
            case LONG -> new MvFirstLongEvaluator.Factory(fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvFirst(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvFirst::new, field());
    }

    @MvEvaluator(extraName = "Boolean")
    static boolean process(BooleanBlock block, int start, int end) {
        return block.getBoolean(start);
    }

    @MvEvaluator(extraName = "Long")
    static long process(LongBlock block, int start, int end) {
        return block.getLong(start);
    }

    @MvEvaluator(extraName = "Int")
    static int process(IntBlock block, int start, int end) {
        return block.getInt(start);
    }

    @MvEvaluator(extraName = "Double")
    static double process(DoubleBlock block, int start, int end) {
        return block.getDouble(start);
    }

    @MvEvaluator(extraName = "BytesRef")
    static BytesRef process(BytesRefBlock block, int start, int end, BytesRef scratch) {
        return block.getBytesRef(start, scratch);
    }
}
