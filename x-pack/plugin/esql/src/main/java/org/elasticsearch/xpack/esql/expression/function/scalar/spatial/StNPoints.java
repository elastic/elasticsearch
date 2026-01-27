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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryPointCountVisitor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Counts the number of points in the geometry
 * Alternatively, it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_NPoints.html">PostGIS:ST_NPoints</a>.
 */
public class StNPoints extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StNPoints",
        StNPoints::new
    );
    private static final GeometryPointCountVisitor pointCounter = new GeometryPointCountVisitor();

    @FunctionInfo(
        returnType = "integer",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        description = "Counts the number of points in the supplied geometry.",
        examples = @Example(file = "spatial_shapes", tag = "st_npoints"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StNPoints(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression field
    ) {
        this(source, field, false);
    }

    private StNPoints(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StNPoints(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return new StNPointsFromPointDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        } else {
            return new StNPointsFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StNPoints(source(), spatialField(), useDocValues);
    }

    @Override
    public DataType dataType() {
        return INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StNPoints(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StNPoints::new, spatialField());
    }

    @Evaluator(extraName = "FromPointDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromPointDocValues(IntBlock.Builder results, @Position int p, LongBlock encoded) {
        results.appendInt(encoded.getValueCount(p));
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(IntBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        BytesRef scratch = new BytesRef();
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        int totalPoints = 0;
        for (int i = 0; i < valueCount; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            Geometry geometry = UNSPECIFIED.wkbToGeometry(wkb);
            Integer count = geometry.visit(pointCounter);
            if (count != null) {
                totalPoints += count;
            }
        }
        results.appendInt(totalPoints);
    }
}
