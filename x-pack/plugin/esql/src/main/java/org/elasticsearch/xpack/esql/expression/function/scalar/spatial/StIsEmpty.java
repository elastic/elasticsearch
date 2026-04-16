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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Returns true if the given geometry is empty.
 * The function {@code st_isempty} is defined in the
 * <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_IsEmpty.html">PostGIS:ST_IsEmpty</a>.
 */
public class StIsEmpty extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StIsEmpty",
        StIsEmpty::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StIsEmpty.class).unary(StIsEmpty::new).name("st_isempty");

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        description = "Returns true if the supplied geometry is empty.\n"
            + "An empty geometry is one that has no points, such as an empty geometry collection or an empty linestring.",
        examples = @Example(file = "spatial_shapes", tag = "st_isempty"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StIsEmpty(
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

    private StIsEmpty(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StIsEmpty(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return new StIsEmptyFromPointDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
        return new StIsEmptyFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StIsEmpty(source(), spatialField(), useDocValues);
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StIsEmpty(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StIsEmpty::new, spatialField());
    }

    @Evaluator(extraName = "FromPointDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromPointDocValues(BooleanBlock.Builder results, @Position int p, LongBlock encoded) {
        // Points in doc values are encoded as longs (packed coordinates) and are never empty.
        // If the field is missing, the generated evaluator handles the null.
        // For multi-valued points, each value is a non-empty encoded point.
        results.appendBoolean(false);
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(BooleanBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        BytesRef scratch = new BytesRef();
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendBoolean(true);
            return;
        }
        // Check if all geometries at this position are empty
        boolean allEmpty = true;
        for (int i = 0; i < valueCount; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            Geometry geometry = UNSPECIFIED.wkbToGeometry(wkb);
            if (geometry.isEmpty() == false) {
                allEmpty = false;
                break;
            }
        }
        results.appendBoolean(allEmpty);
    }
}
