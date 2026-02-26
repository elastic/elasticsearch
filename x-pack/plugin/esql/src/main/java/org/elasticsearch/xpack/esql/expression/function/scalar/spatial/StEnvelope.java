/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.CartesianPointVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.GeoPointVisitor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude.WRAP;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialGeo;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialPoint;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Determines the minimum bounding rectangle of a geometry.
 * The function `st_envelope` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_ENVELOPE.html">PostGIS:ST_ENVELOPE</a>.
 */
public class StEnvelope extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StEnvelope",
        StEnvelope::new
    );
    private static final SpatialEnvelopeResults<BytesRefBlock.Builder> geoResults = new SpatialEnvelopeResults<>(
        SpatialCoordinateTypes.GEO,
        new GeoPointVisitor(WRAP)
    );
    private static final SpatialEnvelopeResults<BytesRefBlock.Builder> cartesianResults = new SpatialEnvelopeResults<>(
        SpatialCoordinateTypes.CARTESIAN,
        new CartesianPointVisitor()
    );
    private DataType dataType;

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Determines the minimum bounding box of the supplied geometry.",
        examples = @Example(file = "spatial_shapes", tag = "st_envelope")
    )
    public StEnvelope(
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

    private StEnvelope(Source source, Expression field, boolean useDocValues) {
        super(source, field, useDocValues);
    }

    private StEnvelope(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StEnvelope(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        var resolution = super.resolveType();
        if (resolution.resolved()) {
            this.dataType = switch (spatialField().dataType()) {
                case GEO_POINT, GEO_SHAPE -> GEO_SHAPE;
                case CARTESIAN_POINT, CARTESIAN_SHAPE -> CARTESIAN_SHAPE;
                default -> NULL;
            };
        }
        return resolution;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // Create the results-builder as a factory, so thread-local instances can be used in evaluators
        var resultsBuilder = isSpatialGeo(spatialField().dataType())
            ? new SpatialEnvelopeResults.Factory<BytesRefBlock.Builder>(GEO, () -> new GeoPointVisitor(WRAP))
            : new SpatialEnvelopeResults.Factory<BytesRefBlock.Builder>(CARTESIAN, CartesianPointVisitor::new);
        var spatial = toEvaluator.apply(spatialField());
        if (spatialDocValues) {
            if (isSpatialPoint(spatialField().dataType())) {
                return new StEnvelopeFromDocValuesEvaluator.Factory(source(), spatial, resultsBuilder::get);
            }
            throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
        }
        return new StEnvelopeFromWKBEvaluator.Factory(source(), spatial, resultsBuilder::get);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StEnvelope(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StEnvelope::new, spatialField());
    }

    static void buildEnvelopeResults(BytesRefBlock.Builder results, Rectangle rectangle, SpatialCoordinateTypes type) {
        long encodedMin = type.pointAsLong(rectangle.getMinX(), rectangle.getMinY());
        long encodedMax = type.pointAsLong(rectangle.getMaxX(), rectangle.getMaxY());
        double minX = type.decodeX(encodedMin);
        double minY = type.decodeY(encodedMin);
        double maxX = type.decodeX(encodedMax);
        double maxY = type.decodeY(encodedMax);
        rectangle = new Rectangle(minX, maxX, maxY, minY);
        results.appendBytesRef(UNSPECIFIED.asWkb(rectangle));
    }

    static void buildDocValuesEnvelopeResults(BytesRefBlock.Builder results, Rectangle rectangle) {
        // We read data from doc-values, so we do not need to quantize again
        results.appendBytesRef(UNSPECIFIED.asWkb(rectangle));
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(
        BytesRefBlock.Builder results,
        @Position int p,
        BytesRefBlock wkbBlock,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) SpatialEnvelopeResults<BytesRefBlock.Builder> resultsBuilder
    ) {
        resultsBuilder.fromWellKnownBinary(results, p, wkbBlock, StEnvelope::buildEnvelopeResults);
    }

    @Evaluator(extraName = "FromDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValues(
        BytesRefBlock.Builder results,
        @Position int p,
        LongBlock encodedBlock,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) SpatialEnvelopeResults<BytesRefBlock.Builder> resultsBuilder
    ) {
        resultsBuilder.fromDocValues(results, p, encodedBlock, StEnvelope::buildDocValuesEnvelopeResults);
    }
}
