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
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * Determines the minimum bounding rectangle of a geometry.
 * The function `st_envelope` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_ENVELOPE.html">PostGIS:ST_ENVELOPE</a>.
 */
public class StEnvelope extends SpatialDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StEnvelope",
        StEnvelope::new
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
        super(source, List.of(field), useDocValues);
    }

    private StEnvelope(StreamInput in) throws IOException {
        this(Source.readFrom((StreamInput & PlanStreamInput) in), in.readNamedWriteable(Expression.class), false);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        var resolution = isSpatial(spatialField(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
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
        if (spatialDocValues) {
            if (spatialField().dataType() == GEO_POINT) {
                return new StEnvelopeFromDocValuesGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
            }
            if (spatialField().dataType() == CARTESIAN_POINT) {
                return new StEnvelopeFromDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
            }
            // Planner mistake: should not have enabled doc values on shapes
            throw new UnsupportedOperationException("StEnvelope does not support doc values on shapes");
        } else {
            if (spatialField().dataType() == GEO_POINT || spatialField().dataType() == DataType.GEO_SHAPE) {
                return new StEnvelopeFromWKBGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
            }
            return new StEnvelopeFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StEnvelope(source(), spatialField(), useDocValues);
    }

    @Override
    public Expression spatialField() {
        return children().getFirst();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(spatialField());
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

    private static void fromWellKnownBinary(
        BytesRefBlock.Builder results,
        @Position int p,
        BytesRefBlock wkbBlock,
        SpatialEnvelopeVisitor.PointVisitor pointVisitor
    ) {
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        var visitor = new SpatialEnvelopeVisitor(pointVisitor);
        for (int i = 0; i < valueCount; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            var geometry = UNSPECIFIED.wkbToGeometry(wkb);
            geometry.visit(visitor);
        }
        if (pointVisitor.isValid()) {
            results.appendBytesRef(UNSPECIFIED.asWkb(visitor.getResult()));
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(BytesRefBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        fromWellKnownBinary(results, p, wkbBlock, new SpatialEnvelopeVisitor.CartesianPointVisitor());
    }

    @Evaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinaryGeo(BytesRefBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        fromWellKnownBinary(results, p, wkbBlock, new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP));
    }

    private static void fromDocValues(
        BytesRefBlock.Builder results,
        @Position int p,
        LongBlock encodedBlock,
        SpatialEnvelopeVisitor.PointVisitor pointVisitor,
        SpatialCoordinateTypes spatialCoordinateType
    ) {
        int firstValueIndex = encodedBlock.getFirstValueIndex(p);
        int valueCount = encodedBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        for (int i = 0; i < valueCount; i++) {
            long encoded = encodedBlock.getLong(firstValueIndex + i);
            var point = spatialCoordinateType.longAsPoint(encoded);
            pointVisitor.visitPoint(point.getX(), point.getY());
        }
        if (pointVisitor.isValid()) {
            results.appendBytesRef(UNSPECIFIED.asWkb(pointVisitor.getResult()));
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @Evaluator(extraName = "FromDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValues(BytesRefBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        fromDocValues(results, p, encodedBlock, new SpatialEnvelopeVisitor.CartesianPointVisitor(), SpatialCoordinateTypes.CARTESIAN);
    }

    @Evaluator(extraName = "FromDocValuesGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValuesGeo(BytesRefBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        fromDocValues(results, p, encodedBlock, new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP), SpatialCoordinateTypes.GEO);
    }
}
