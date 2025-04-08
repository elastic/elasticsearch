/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

public class StGeohashToGeoShape extends AbstractConvertFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohashToGeoShape",
        StGeohashToGeoShape::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, StGeohashToGeoShapeFromStringEvaluator.Factory::new),
        Map.entry(LONG, StGeohashToGeoShapeFromLongEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "geo_shape",
        description = """
            Converts an input value to a `geo_shape` value.
            The input values are expected to be the grid-ids of geohash grids, in either long or string format.""",
        examples = @Example(file = "spatial-grid", tag = "geohash_to_geoshape")
    )
    public StGeohashToGeoShape(
        Source source,
        @Param(
            name = "grid_id",
            type = { "keyword", "long" },
            description = "Input geohash grid-id. The input can be a single- or multi-valued column or an expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private StGeohashToGeoShape(StreamInput in) throws IOException {
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
        return GEO_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StGeohashToGeoShape(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohashToGeoShape::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static BytesRef fromString(BytesRef gridId) {
        Rectangle bbox = Geohash.toBoundingBox(gridId.utf8ToString());
        return SpatialCoordinateTypes.GEO.asWkb(bbox);
    }

    @ConvertEvaluator(extraName = "FromLong")
    static BytesRef fromLong(long gridId) {
        Rectangle bbox = Geohash.toBoundingBox(Geohash.stringEncode(gridId));
        return SpatialCoordinateTypes.GEO.asWkb(bbox);
    }
}
