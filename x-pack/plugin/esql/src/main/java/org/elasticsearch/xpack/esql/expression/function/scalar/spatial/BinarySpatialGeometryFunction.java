/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialBinaryGeometryBlockProcessor.flattenIfHeterogeneousCollection;

/**
 * Abstract base for spatial functions that combine two geometry arguments and return a new geometry
 * (e.g. ST_UNION, ST_INTERSECTION, ST_DIFFERENCE, ST_SYMDIFFERENCE).
 * <p>
 * Unlike {@link SpatialRelatesFunction}, these functions are not predicates and cannot be pushed
 * down to Lucene. They are expected to appear in EVAL commands and return {@code geo_shape} or
 * {@code cartesian_shape} results.
 * </p>
 * <p>
 * Both arguments must be spatial and must share the same coordinate reference system (geo or
 * cartesian). Either or both arguments may be point fields accessed via doc-values; in that case
 * the function is marked using {@link #withDocValues(boolean, boolean)} during local physical
 * planning so that the correct evaluator variant is chosen.
 * </p>
 */
public abstract class BinarySpatialGeometryFunction extends EsqlScalarFunction {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    protected final Expression left;
    protected final Expression right;
    protected final boolean leftDocValues;
    protected final boolean rightDocValues;

    protected BinarySpatialGeometryFunction(
        Source source,
        Expression left,
        Expression right,
        boolean leftDocValues,
        boolean rightDocValues
    ) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
        this.leftDocValues = leftDocValues;
        this.rightDocValues = rightDocValues;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
        // leftDocValues / rightDocValues are local physical-planning state and are never serialized
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public boolean leftDocValues() {
        return leftDocValues;
    }

    public boolean rightDocValues() {
        return rightDocValues;
    }

    /**
     * Return a copy of this function with the specified doc-values flags set.
     * Only point types support doc-values access; shape types are always read from source.
     */
    public abstract BinarySpatialGeometryFunction withDocValues(boolean foundLeft, boolean foundRight);

    @Override
    public DataType dataType() {
        if (isNull(left.dataType()) || isNull(right.dataType())) {
            return DataType.NULL;
        }
        return DataType.isSpatialGeo(left.dataType()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public Object fold(FoldContext ctx) {
        Object leftVal = left.fold(ctx);
        Object rightVal = right.fold(ctx);
        if (leftVal == null || rightVal == null) {
            return null;
        }
        try {
            Geometry leftJts = toJts(leftVal);
            Geometry rightJts = toJts(rightVal);
            return UNSPECIFIED.jtsGeometryToWkb(jtsOperation(leftJts, rightJts));
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
        }
    }

    /**
     * Apply the concrete JTS geometry operation (union, intersection, difference, or symDifference).
     */
    protected abstract Geometry jtsOperation(Geometry leftGeom, Geometry rightGeom);

    @Override
    protected TypeResolution resolveType() {
        if (isNull(left.dataType())) {
            return isSpatial(right, sourceText(), SECOND);
        }
        if (isNull(right.dataType())) {
            return isSpatial(left, sourceText(), FIRST);
        }
        TypeResolution leftResolution = isSpatial(left, sourceText(), FIRST);
        if (leftResolution.resolved()) {
            return isType(
                right,
                dt -> DataType.isSpatial(dt) && spatialCRSCompatible(left.dataType(), dt),
                sourceText(),
                SECOND,
                compatibleTypeNames(left.dataType())
            );
        }
        TypeResolution rightResolution = isSpatial(right, sourceText(), SECOND);
        if (rightResolution.resolved()) {
            return isType(
                left,
                dt -> DataType.isSpatial(dt) && spatialCRSCompatible(right.dataType(), dt),
                sourceText(),
                FIRST,
                compatibleTypeNames(right.dataType())
            );
        }
        return leftResolution;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), leftDocValues, rightDocValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinarySpatialGeometryFunction other = (BinarySpatialGeometryFunction) obj;
            return Objects.equals(other.leftDocValues, leftDocValues) && Objects.equals(other.rightDocValues, rightDocValues);
        }
        return false;
    }

    private static Geometry toJts(Object value) throws ParseException {
        return switch (value) {
            case BytesRef wkb -> flattenIfHeterogeneousCollection(UNSPECIFIED.wkbToJtsGeometry(wkb));
            case List<?> list -> {
                List<Geometry> geometries = new ArrayList<>(list.size());
                for (Object item : list) {
                    if (item instanceof BytesRef wkb) {
                        geometries.add(UNSPECIFIED.wkbToJtsGeometry(wkb));
                    } else {
                        throw new IllegalArgumentException("unsupported list element type: " + item.getClass().getSimpleName());
                    }
                }
                yield UnaryUnionOp.union(geometries);
            }
            default -> throw new IllegalArgumentException("unsupported geometry type: " + value.getClass().getSimpleName());
        };
    }

    static boolean spatialCRSCompatible(DataType a, DataType b) {
        return DataType.isSpatialGeo(a) == DataType.isSpatialGeo(b);
    }

    static String[] compatibleTypeNames(DataType dt) {
        return DataType.isSpatialGeo(dt)
            ? new String[] { DataType.GEO_POINT.typeName(), DataType.GEO_SHAPE.typeName() }
            : new String[] { DataType.CARTESIAN_POINT.typeName(), DataType.CARTESIAN_SHAPE.typeName() };
    }
}
