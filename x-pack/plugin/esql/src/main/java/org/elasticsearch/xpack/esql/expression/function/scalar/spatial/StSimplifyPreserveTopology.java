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
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

public class StSimplifyPreserveTopology extends SpatialDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StSimplifyPreserveTopology",
        StSimplifyPreserveTopology::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StSimplifyPreserveTopology.class)
        .binary(StSimplifyPreserveTopology::new)
        .name("st_simplifypreservetopology");
    private static final SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(
        UNSPECIFIED,
        TopologyPreservingSimplifier::simplify
    );
    private static final SpatialGeometryBlockProcessor geoProcessor = new SpatialGeometryBlockProcessor(
        GEO,
        TopologyPreservingSimplifier::simplify
    );
    private static final SpatialGeometryBlockProcessor cartesianProcessor = new SpatialGeometryBlockProcessor(
        CARTESIAN,
        TopologyPreservingSimplifier::simplify
    );
    private final Expression geometry;
    private final Expression tolerance;

    @FunctionInfo(
        returnType = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
        description = "Simplifies the input geometry by applying a topology-preserving variant of the Douglas-Peucker algorithm "
            + "with a specified tolerance. "
            + "Vertices that fall within the tolerance distance from the simplified shape are removed. "
            + "Unlike `ST_SIMPLIFY`, the resulting geometry is guaranteed to be topologically valid.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        examples = @Example(file = "spatial-jts", tag = "st_simplifypreservetopology"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StSimplifyPreserveTopology(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression geometry,
        @Param(
            name = "tolerance",
            type = { "double", "float", "long", "integer" },
            description = "Tolerance for the geometry simplification, in the units of the input SRS"
        ) Expression tolerance
    ) {
        this(source, geometry, tolerance, false);
    }

    private StSimplifyPreserveTopology(Source source, Expression geometry, Expression tolerance, boolean spatialDocValues) {
        super(source, List.of(geometry, tolerance), spatialDocValues);
        this.geometry = geometry;
        this.tolerance = tolerance;
    }

    private StSimplifyPreserveTopology(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        return tolerance.dataType() == DataType.NULL ? DataType.NULL : geometry.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StSimplifyPreserveTopology(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StSimplifyPreserveTopology::new, geometry, tolerance);
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

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StSimplifyPreserveTopology(source(), geometry, tolerance, useDocValues);
    }

    @Override
    public Expression spatialField() {
        return geometry;
    }

    Expression tolerance() {
        return tolerance;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        TypeResolution spatialResolved = isSpatial(geometry, sourceText(), FIRST);
        if (spatialResolved.unresolved()) {
            return spatialResolved;
        }
        return isType(
            tolerance,
            t -> t == DOUBLE || t == FLOAT || t == LONG || t == INTEGER,
            sourceText(),
            SECOND,
            "double",
            "float",
            "long",
            "integer"
        );
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory geometryEvaluator = toEvaluator.apply(geometry);

        if (tolerance.foldable() == false) {
            throw new IllegalArgumentException("tolerance must be foldable");
        }
        var toleranceExpression = tolerance.fold(toEvaluator.foldCtx());
        double inputTolerance = StSimplify.getInputTolerance(toleranceExpression);
        if (spatialDocValues && geometry.dataType() == DataType.GEO_POINT) {
            return new StSimplifyPreserveTopologyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        } else if (spatialDocValues && geometry.dataType() == DataType.CARTESIAN_POINT) {
            return new StSimplifyPreserveTopologyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        }
        return new StSimplifyPreserveTopologyNonFoldableGeometryAndFoldableToleranceEvaluator.Factory(
            source(),
            geometryEvaluator,
            inputTolerance
        );
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && tolerance.foldable();
    }

    @Override
    public Object fold(FoldContext foldCtx) {
        var toleranceExpression = tolerance.fold(foldCtx);
        double inputTolerance = StSimplify.getInputTolerance(toleranceExpression);
        Object input = geometry.fold(foldCtx);
        return switch (input) {
            case null -> null;
            case List<?> list -> processor.processSingleGeometry(processor.asJtsGeometry(list), inputTolerance);
            case BytesRef inputGeometry -> processor.processSingleGeometry(inputGeometry, inputTolerance);
            default -> throw new IllegalArgumentException("unsupported block type: " + input.getClass().getSimpleName());
        };
    }

    @Evaluator(extraName = "NonFoldableGeometryAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static void processNonFoldableGeometryAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double tolerance
    ) {
        processor.processGeometries(builder, p, geometry, tolerance);
    }

    @Evaluator(
        extraName = "NonFoldableGeoPointDocValuesAndFoldableTolerance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processGeoPointDocValuesAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock point,
        @Fixed double tolerance
    ) throws IOException {
        geoProcessor.processPoints(builder, p, point, tolerance);
    }

    @Evaluator(
        extraName = "NonFoldableCartesianPointDocValuesAndFoldableTolerance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processCartesianPointDocValuesAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock left,
        @Fixed double tolerance
    ) throws IOException {
        cartesianProcessor.processPoints(builder, p, left, tolerance);
    }
}
