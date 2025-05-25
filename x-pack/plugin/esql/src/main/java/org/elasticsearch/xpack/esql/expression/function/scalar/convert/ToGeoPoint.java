/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToGeo;

public class ToGeoPoint extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToGeoPoint",
        ToGeoPoint::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(GEO_POINT, (source, fieldEval) -> fieldEval),
        Map.entry(KEYWORD, ToGeoPointFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToGeoPointFromStringEvaluator.Factory::new),
        Map.entry(SEMANTIC_TEXT, ToGeoPointFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "geo_point",
        description = """
            Converts an input value to a `geo_point` value.
            A string will only be successfully converted if it respects the
            {wikipedia}/Well-known_text_representation_of_geometry[WKT Point] format.""",
        examples = @Example(file = "spatial", tag = "to_geopoint-str")
    )
    public ToGeoPoint(
        Source source,
        @Param(
            name = "field",
            type = { "geo_point", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToGeoPoint(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return GEO_POINT;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToGeoPoint(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToGeoPoint::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef in) {
        return stringToGeo(in.utf8ToString());
    }
}
