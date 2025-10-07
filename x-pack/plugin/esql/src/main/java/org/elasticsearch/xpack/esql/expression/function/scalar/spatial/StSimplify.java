/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.makeGeometryFromLiteral;

public class StSimplify extends ScalarFunction implements EvaluatorMapper {
    Expression geometry;
    Expression tolerance;

    @FunctionInfo(
        returnType = "geo_shape",
        description = "Simplifies the input geometry with a given tolerance",
        examples = @Example(file = "spatial", tag = "st_simplify")
    )
    public StSimplify(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression geometry,
        @Param(name = "tolerance", type = { "double" }, description = "Tolerance for the geometry simplification") Expression tolerance
    ) {
        super(source, List.of(geometry, tolerance));
        this.geometry = geometry;
        this.tolerance = tolerance;
    }

    @Override
    public DataType dataType() {
        return GEO_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StSimplify(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StSimplify::new, geometry, tolerance);
    }

    @Override
    public String getWriteableName() {
        return "StSimplify";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // Get evaluators for the child expressions
        EvalOperator.ExpressionEvaluator.Factory geometryEval = toEvaluator.apply(geometry);
        EvalOperator.ExpressionEvaluator.Factory toleranceEval = toEvaluator.apply(tolerance);

        return dvrCtx -> {
            EvalOperator.ExpressionEvaluator geometryEvaluator = geometryEval.get(dvrCtx);
            EvalOperator.ExpressionEvaluator toleranceEvaluator = toleranceEval.get(dvrCtx);

            return new EvalOperator.ExpressionEvaluator() {
                @Override
                public void close() {

                }

                @Override
                public Block eval(Page page) {
                    var isGeometryFoldable = geometry.foldable();
                    var isToleranceFoldable = tolerance.foldable();
                    var positionCount = page.getPositionCount();

                    if (isGeometryFoldable) {
                        var esGeometry = makeGeometryFromLiteral(toEvaluator.foldCtx(), geometry);
                        String wkt = WellKnownText.toWKT(esGeometry);
                        WKTReader reader = new WKTReader();

                        try (var result = dvrCtx.blockFactory().newBytesRefVectorBuilder(positionCount)) {
                            for (int p = 0; p < positionCount; p++) {
                                try {
                                    org.locationtech.jts.geom.Geometry jtsGeometry = reader.read(wkt);
                                    org.locationtech.jts.geom.Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(
                                        jtsGeometry,
                                        0
                                    );
                                    WKTWriter writer = new WKTWriter();
                                    String simplifiedWkt = writer.write(simplifiedGeometry);
                                    Geometry esGeometryResult = WellKnownText.fromWKT(
                                        StandardValidator.instance(true),
                                        false,
                                        simplifiedWkt
                                    );
                                    result.appendBytesRef(new BytesRef(WellKnownBinary.toWKB(esGeometryResult, ByteOrder.LITTLE_ENDIAN)));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            return result.build().asBlock();
                        }
                    }
                    // identity
                    return geometryEvaluator.eval(page);
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }
            };
        };
    }
}
