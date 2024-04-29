/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

public class ToCartesianShape extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(CARTESIAN_POINT, (fieldEval, source) -> fieldEval),
        Map.entry(CARTESIAN_SHAPE, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, ToCartesianShapeFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToCartesianShapeFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "cartesian_shape",
        description = """
            Converts an input value to a `cartesian_shape` value.
            A string will only be successfully converted if it respects the
            {wikipedia}/Well-known_text_representation_of_geometry[WKT] format.""",
        examples = @Example(file = "spatial_shapes", tag = "to_cartesianshape-str")
    )
    public ToCartesianShape(
        Source source,
        @Param(
            name = "field",
            type = { "cartesian_point", "cartesian_shape", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return CARTESIAN_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToCartesianShape(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToCartesianShape::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef in) {
        return stringToSpatial(in.utf8ToString());
    }
}
