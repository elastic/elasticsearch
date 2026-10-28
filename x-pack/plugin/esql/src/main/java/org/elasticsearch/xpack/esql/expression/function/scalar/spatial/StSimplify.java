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
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.locationtech.jts.geom.GeometryFactory;

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

public class StSimplify extends SpatialDocValuesFunction implements AnyNullIsNull, BlockLoaderExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StSimplify",
        StSimplify::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StSimplify.class)
        .binary(StSimplify::new)
        // Results are quantized to the Lucene grid, matching the coordinates of shape fields simplified from doc values
        .capabilities("doc_values")
        .name("st_simplify");
    private static final SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(
        UNSPECIFIED,
        IterativeDouglasPeuckerSimplifier::simplify
    );
    private static final SpatialGeometryBlockProcessor geoProcessor = new SpatialGeometryBlockProcessor(
        GEO,
        IterativeDouglasPeuckerSimplifier::simplify
    );
    private static final SpatialGeometryBlockProcessor cartesianProcessor = new SpatialGeometryBlockProcessor(
        CARTESIAN,
        IterativeDouglasPeuckerSimplifier::simplify
    );
    private final Expression geometry;
    private final Expression tolerance;

    @FunctionInfo(
        returnType = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
        briefSummary = "Simplifies the input geometry using the Douglas-Peucker algorithm with a specified tolerance.",
        description = "Simplifies the input geometry by applying the Douglas-Peucker algorithm with a specified tolerance. "
            + "Vertices that fall within the tolerance distance from the simplified shape are removed. "
            + "Note that the resulting geometry may be invalid, even if the original input was valid.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        examples = @Example(file = "spatial-jts", tag = "st_simplify"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StSimplify(
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
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Tolerance for the geometry simplification, in the units of the input SRS"
        ) Expression tolerance
    ) {
        this(source, geometry, tolerance, false);
    }

    private StSimplify(Source source, Expression geometry, Expression tolerance, boolean spatialDocValues) {
        super(source, List.of(geometry, tolerance), spatialDocValues);
        this.geometry = geometry;
        this.tolerance = tolerance;
    }

    private StSimplify(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        return tolerance.dataType() == DataType.NULL ? DataType.NULL : geometry.dataType();
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

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StSimplify(source(), geometry, tolerance, useDocValues);
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
        double inputTolerance = getInputTolerance(toleranceExpression);
        if (spatialDocValues && geometry.dataType() == DataType.GEO_POINT) {
            return new StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        } else if (spatialDocValues && geometry.dataType() == DataType.CARTESIAN_POINT) {
            return new StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        }
        // For WKB, we need to know if it is Geo or Cartesian to quantize the results to match Lucene grids
        return DataType.isSpatialGeo(geometry.dataType())
            ? new StSimplifyNonFoldableGeoShapeAndFoldableToleranceEvaluator.Factory(source(), geometryEvaluator, inputTolerance)
            : new StSimplifyNonFoldableCartesianShapeAndFoldableToleranceEvaluator.Factory(source(), geometryEvaluator, inputTolerance);
    }

    /**
     * Fuses this function into the loading of a {@code geo_shape} or {@code cartesian_shape} field whose doc values can
     * reconstruct the original geometry, so the geometry is converted straight from the doc values into JTS and only
     * the simplified result is written as WKB. See {@link BlockLoaderExpression} for the general mechanism. A tolerance
     * that is not a non-negative constant number keeps using the evaluator so that it reports the error. Like the
     * evaluator on doc-values input, the input coordinates are those of the doc values, already on the Lucene grid.
     */
    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        if (geometry instanceof FieldAttribute field
            && (field.dataType() == DataType.GEO_SHAPE || field.dataType() == DataType.CARTESIAN_SHAPE)
            && tolerance instanceof Literal literal
            && literal.value() instanceof Number number
            && number.doubleValue() >= 0
            && stats.hasDocValues(field.fieldName())
            && stats.supportsGeometryDocValueReconstruction(field.fieldName())) {
            double inputTolerance = number.doubleValue();
            return new PushedBlockLoaderExpression(
                field,
                new BlockLoaderFunctionConfig.GeometryTransform(
                    BlockLoaderFunctionConfig.Function.ST_SIMPLIFY,
                    inputTolerance,
                    simplifyTransformers(inputTolerance)
                )
            );
        }
        return null;
    }

    private static BlockLoaderFunctionConfig.GeometryTransformerFactory simplifyTransformers(double tolerance) {
        return warnings -> {
            JtsGeometryConverter converter = new JtsGeometryConverter(new GeometryFactory());
            return geometry -> {
                try {
                    return processor.processSingleGeometry(converter.toJts(geometry), tolerance);
                } catch (IllegalArgumentException e) {
                    if (warnings != null) {
                        warnings.registerException(IllegalArgumentException.class, e.getMessage());
                    }
                    return null;
                }
            };
        };
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && tolerance.foldable();
    }

    @Override
    public Object fold(FoldContext foldCtx) {
        var toleranceExpression = tolerance.fold(foldCtx);
        double inputTolerance = getInputTolerance(toleranceExpression);
        Object input = geometry.fold(foldCtx);
        return switch (input) {
            case null -> null;
            case List<?> list -> processor.processSingleGeometry(processor.asJtsGeometry(list), inputTolerance);
            case BytesRef inputGeometry -> processor.processSingleGeometry(inputGeometry, inputTolerance);
            default -> throw new IllegalArgumentException("unsupported block type: " + input.getClass().getSimpleName());
        };
    }

    @Evaluator(extraName = "NonFoldableGeoShapeAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static void processGeoShapeAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double tolerance
    ) {
        geoProcessor.processGeometries(builder, p, geometry, tolerance);
    }

    @Evaluator(extraName = "NonFoldableCartesianShapeAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static void processCartesianShapeAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double tolerance
    ) {
        cartesianProcessor.processGeometries(builder, p, geometry, tolerance);
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

    static double getInputTolerance(Object toleranceExpression) {
        double inputTolerance;

        if (toleranceExpression instanceof Number number) {
            inputTolerance = number.doubleValue();
        } else {
            throw new IllegalArgumentException("tolerance for st_simplify must be an integer or floating-point number");
        }

        if (inputTolerance < 0) {
            throw new IllegalArgumentException("tolerance must not be negative");
        }
        return inputTolerance;
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
