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
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
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

public class StGeohexToGeoShape extends AbstractConvertFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohexToGeoShape",
        StGeohexToGeoShape::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, StGeohexToGeoShapeFromStringEvaluator.Factory::new),
        Map.entry(LONG, StGeohexToGeoShapeFromLongEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "geo_shape",
        description = """
            Converts an input value to a `geo_shape` value.
            The input values are expected to be the grid-ids of H3 grids, in either long or string format.""",
        examples = @Example(file = "spatial-grid", tag = "geohex_to_geoshape"),
        depthOffset = 1 // make it appear as a subsection of ST_GEOHEX
    )
    public StGeohexToGeoShape(
        Source source,
        @Param(
            name = "grid_id",
            type = { "keyword", "long" },
            description = "Input H3 grid-id. The input can be a single- or multi-valued column or an expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private StGeohexToGeoShape(StreamInput in) throws IOException {
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
        return new StGeohexToGeoShape(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohexToGeoShape::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static BytesRef fromString(BytesRef gridId) {
        return fromCellBoundary(H3.h3ToGeoBoundary(gridId.utf8ToString()));
    }

    @ConvertEvaluator(extraName = "FromLong")
    static BytesRef fromLong(long gridId) {
        return fromCellBoundary(H3.h3ToGeoBoundary(gridId));
    }

    private static BytesRef fromCellBoundary(CellBoundary cell) {
        double[] x = new double[cell.numPoints() + 1];
        double[] y = new double[cell.numPoints() + 1];
        for (int i = 0; i < cell.numPoints(); i++) {
            LatLng vertex = cell.getLatLon(i);
            x[i] = vertex.getLonDeg();
            y[i] = vertex.getLatDeg();
        }
        x[cell.numPoints()] = x[0];
        y[cell.numPoints()] = y[0];
        LinearRing ring = new LinearRing(x, y);
        Polygon polygon = new Polygon(ring);
        return SpatialCoordinateTypes.GEO.asWkb(polygon);
    }
}
