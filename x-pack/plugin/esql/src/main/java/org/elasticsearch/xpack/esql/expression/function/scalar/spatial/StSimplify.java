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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class StSimplify extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StSimplify",
        StSimplify::new
    );
    Expression geometry;
    Expression tolerance;

    @FunctionInfo(
        returnType = "geo_shape",
        description = "Simplifies the input geometry with a given tolerance.",
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

    private StSimplify(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
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
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(geometry);
        out.writeNamedWriteable(tolerance);
    }

    private static BytesRef geoSourceAndConstantTolerance(BytesRef inputGeometry, double inputTolerance) {
        if (inputGeometry == null) {
            return null;
        }
        try {
            org.locationtech.jts.geom.Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(inputGeometry);
            org.locationtech.jts.geom.Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, inputTolerance);
            return UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry);
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e);
        }
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory geometryEvaluator = toEvaluator.apply(geometry);

        if (tolerance.foldable() == false) {
            throw new IllegalArgumentException("tolerance must be foldable");
        }
        var toleranceExpression = tolerance.fold(toEvaluator.foldCtx());
        double inputTolerance = getInputTolerance(toleranceExpression);
        if (geometry.foldable()) {
            BytesRef inputGeometry = (BytesRef) geometry.fold(toEvaluator.foldCtx());
            return new StSimplifyFoldableGeoAndFoldableToleranceEvaluator.Factory(source(), inputGeometry, inputTolerance);
        }
        return new StSimplifyNonFoldableGeoAndFoldableToleranceEvaluator.Factory(source(), geometryEvaluator, inputTolerance);
    }

    private static double getInputTolerance(Object toleranceExpression) {
        double inputTolerance;

        if (toleranceExpression instanceof Integer) {
            inputTolerance = ((Integer) toleranceExpression).doubleValue();
        } else if (toleranceExpression instanceof Double) {
            inputTolerance = (double) toleranceExpression;
        } else {
            throw new IllegalArgumentException("tolerance for st_simplify must be a non negative integer or double");
        }

        if (inputTolerance < 0) {
            throw new IllegalArgumentException("tolerance must not be negative");
        }
        return inputTolerance;
    }

    @Evaluator(extraName = "NonFoldableGeoAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static BytesRef processNonFoldableGeoAndConstantTolerance(BytesRef inputGeometry, @Fixed double inputTolerance) {
        return geoSourceAndConstantTolerance(inputGeometry, inputTolerance);
    }

    @Evaluator(extraName = "FoldableGeoAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static BytesRef processFoldableGeoAndFoldableTolerance(@Fixed BytesRef inputGeometry, @Fixed double inputTolerance) {
        return geoSourceAndConstantTolerance(inputGeometry, inputTolerance);
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && tolerance.foldable();
    }
}
