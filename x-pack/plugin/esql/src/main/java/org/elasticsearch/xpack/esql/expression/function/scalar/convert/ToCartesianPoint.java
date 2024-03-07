/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.qlcore.util.SpatialCoordinateTypes.CARTESIAN;

public class ToCartesianPoint extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(CARTESIAN_POINT, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, ToCartesianPointFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToCartesianPointFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "cartesian_point", description = "Converts an input value to a point value.")
    public ToCartesianPoint(Source source, @Param(name = "v", type = { "cartesian_point", "keyword", "text" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return CARTESIAN_POINT;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToCartesianPoint(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToCartesianPoint::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef in) {
        return CARTESIAN.wktToWkb(in.utf8ToString());
    }
}
