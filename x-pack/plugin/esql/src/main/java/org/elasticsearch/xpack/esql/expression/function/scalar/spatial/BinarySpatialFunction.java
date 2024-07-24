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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;

/**
 * Spatial functions that take two arguments that must both be spatial types can inherit from this class.
 * This provides common support for type resolution and validation. Ensuring that both arguments are spatial types
 * and of compatible CRS. For example geo_point and geo_shape can be compared, but not geo_point and cartesian_point.
 */
public abstract class BinarySpatialFunction extends BinaryScalarFunction implements SpatialEvaluatorFactory.SpatialSourceResolution {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(SpatialContains.ENTRY, SpatialDisjoint.ENTRY, SpatialIntersects.ENTRY, SpatialWithin.ENTRY, StDistance.ENTRY);
    }

    private final SpatialTypeResolver spatialTypeResolver;
    protected SpatialCrsType crsType;
    protected final boolean leftDocValues;
    protected final boolean rightDocValues;

    protected BinarySpatialFunction(
        Source source,
        Expression left,
        Expression right,
        boolean leftDocValues,
        boolean rightDocValues,
        boolean pointsOnly
    ) {
        super(source, left, right);
        this.leftDocValues = leftDocValues;
        this.rightDocValues = rightDocValues;
        this.spatialTypeResolver = new SpatialTypeResolver(this, pointsOnly);
    }

    protected BinarySpatialFunction(StreamInput in, boolean leftDocValues, boolean rightDocValues, boolean pointsOnly) throws IOException {
        this(
            Source.EMPTY,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            leftDocValues,
            rightDocValues,
            pointsOnly
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
    }

    @Override
    protected TypeResolution resolveType() {
        return spatialTypeResolver.resolveType();
    }

    static class SpatialTypeResolver {
        private final SpatialEvaluatorFactory.SpatialSourceResolution supplier;
        private final boolean pointsOnly;

        SpatialTypeResolver(SpatialEvaluatorFactory.SpatialSourceResolution supplier, boolean pointsOnly) {
            this.supplier = supplier;
            this.pointsOnly = pointsOnly;
        }

        public Expression left() {
            return supplier.left();
        }

        public Expression right() {
            return supplier.right();
        }

        public String sourceText() {
            return supplier.source().text();
        }

        protected TypeResolution resolveType() {
            if (left().foldable() && right().foldable() == false || isNull(left().dataType())) {
                // Left is literal, but right is not, check the left field's type against the right field
                return resolveType(right(), left(), SECOND, FIRST);
            } else {
                // All other cases check the right against the left
                return resolveType(left(), right(), FIRST, SECOND);
            }
        }

        protected Expression.TypeResolution isSpatial(Expression e, TypeResolutions.ParamOrdinal paramOrd) {
            return pointsOnly
                ? EsqlTypeResolutions.isSpatialPoint(e, sourceText(), paramOrd)
                : EsqlTypeResolutions.isSpatial(e, sourceText(), paramOrd);
        }

        private TypeResolution resolveType(
            Expression leftExpression,
            Expression rightExpression,
            TypeResolutions.ParamOrdinal leftOrdinal,
            TypeResolutions.ParamOrdinal rightOrdinal
        ) {
            TypeResolution leftResolution = isSpatial(leftExpression, leftOrdinal);
            TypeResolution rightResolution = isSpatial(rightExpression, rightOrdinal);
            if (leftResolution.resolved()) {
                return resolveType(leftExpression, rightExpression, rightOrdinal);
            } else if (rightResolution.resolved()) {
                return resolveType(rightExpression, leftExpression, leftOrdinal);
            } else {
                return leftResolution;
            }
        }

        protected TypeResolution resolveType(
            Expression spatialExpression,
            Expression otherExpression,
            TypeResolutions.ParamOrdinal otherParamOrdinal
        ) {
            if (isNull(spatialExpression.dataType())) {
                return isSpatial(otherExpression, otherParamOrdinal);
            }
            TypeResolution resolution = isSameSpatialType(spatialExpression.dataType(), otherExpression, sourceText(), otherParamOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            supplier.setCrsType(spatialExpression.dataType());
            return TypeResolution.TYPE_RESOLVED;
        }

        protected TypeResolution isSameSpatialType(
            DataType spatialDataType,
            Expression expression,
            String operationName,
            TypeResolutions.ParamOrdinal paramOrd
        ) {
            return pointsOnly
                ? isType(expression, dt -> dt == spatialDataType, operationName, paramOrd, compatibleTypeNames(spatialDataType))
                : isType(
                    expression,
                    dt -> DataType.isSpatial(dt) && spatialCRSCompatible(spatialDataType, dt),
                    operationName,
                    paramOrd,
                    compatibleTypeNames(spatialDataType)
                );
        }
    }

    @Override
    public void setCrsType(DataType dataType) {
        crsType = SpatialCrsType.fromDataType(dataType);
    }

    private static final String[] GEO_TYPE_NAMES = new String[] { GEO_POINT.typeName(), GEO_SHAPE.typeName() };
    private static final String[] CARTESIAN_TYPE_NAMES = new String[] { GEO_POINT.typeName(), GEO_SHAPE.typeName() };

    protected static boolean spatialCRSCompatible(DataType spatialDataType, DataType otherDataType) {
        return DataType.isSpatialGeo(spatialDataType) && DataType.isSpatialGeo(otherDataType)
            || DataType.isSpatialGeo(spatialDataType) == false && DataType.isSpatialGeo(otherDataType) == false;
    }

    static String[] compatibleTypeNames(DataType spatialDataType) {
        return DataType.isSpatialGeo(spatialDataType) ? GEO_TYPE_NAMES : CARTESIAN_TYPE_NAMES;
    }

    @Override
    public SpatialCrsType crsType() {
        if (crsType == null) {
            resolveType();
        }
        return crsType;
    }

    public boolean leftDocValues() {
        return leftDocValues;
    }

    public boolean rightDocValues() {
        return rightDocValues;
    }

    /**
     * For most spatial functions we only need to know if the CRS is geo or cartesian, not whether the type is point or shape.
     * This enum captures this knowledge.
     */
    public enum SpatialCrsType {
        GEO,
        CARTESIAN,
        UNSPECIFIED;

        public static SpatialCrsType fromDataType(DataType dataType) {
            return DataType.isSpatialGeo(dataType) ? SpatialCrsType.GEO
                : DataType.isSpatial(dataType) ? SpatialCrsType.CARTESIAN
                : SpatialCrsType.UNSPECIFIED;
        }
    }

    protected abstract static class BinarySpatialComparator<T> {
        protected final SpatialCoordinateTypes spatialCoordinateType;
        protected final CoordinateEncoder coordinateEncoder;
        protected final SpatialCrsType crsType;

        protected BinarySpatialComparator(SpatialCoordinateTypes spatialCoordinateType, CoordinateEncoder encoder) {
            this.spatialCoordinateType = spatialCoordinateType;
            this.coordinateEncoder = encoder;
            this.crsType = spatialCoordinateType.equals(SpatialCoordinateTypes.GEO) ? SpatialCrsType.GEO : SpatialCrsType.CARTESIAN;
        }

        protected Geometry fromBytesRef(BytesRef bytesRef) {
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
        }

        protected abstract T compare(BytesRef left, BytesRef right) throws IOException;
    }
}
