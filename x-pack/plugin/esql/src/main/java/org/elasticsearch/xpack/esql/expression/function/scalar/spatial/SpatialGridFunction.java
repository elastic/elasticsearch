/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Spatial functions that take one spatial argument, one parameter and one optional bounds can inherit from this class.
 * Obvious choices are: StGeohash, StGeotile and StGeohex.
 */
public abstract class SpatialGridFunction extends ScalarFunction implements OptionalArgument {
    protected final Expression spatialField;
    protected final Expression parameter;
    protected final Expression bounds;
    protected final boolean spatialDocsValues;

    protected SpatialGridFunction(
        Source source,
        Expression spatialField,
        Expression parameter,
        Expression bounds,
        boolean spatialDocsValues
    ) {
        super(source, bounds == null ? Arrays.asList(spatialField, parameter) : Arrays.asList(spatialField, parameter, bounds));
        this.spatialField = spatialField;
        this.parameter = parameter;
        this.bounds = bounds;
        this.spatialDocsValues = spatialDocsValues;
    }

    protected SpatialGridFunction(StreamInput in, boolean spatialDocsValues) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            spatialDocsValues
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(spatialField);
        out.writeNamedWriteable(parameter);
        out.writeOptionalNamedWriteable(bounds);
    }

    /**
     * Mark the function as expecting the specified field to arrive as doc-values.
     * This only applies to geo_point and cartesian_point types.
     */
    public abstract SpatialGridFunction withDocValues(boolean useDocValues);

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isGeoPoint(spatialField(), sourceText());
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isWholeNumber(parameter(), sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (bounds() != null) {
            resolution = isGeoShape(bounds(), sourceText());
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    protected static Expression.TypeResolution isGeoPoint(Expression e, String operationName) {
        return isType(e, t -> t.equals(GEO_POINT), operationName, FIRST, GEO_POINT.typeName());
    }

    protected static Expression.TypeResolution isGeoShape(Expression e, String operationName) {
        return isType(e, t -> t.equals(GEO_SHAPE), operationName, THIRD, GEO_SHAPE.typeName());
    }

    protected static Rectangle asRectangle(BytesRef boundsBytesRef) {
        var geometry = GEO.wkbToGeometry(boundsBytesRef);
        if (geometry instanceof Rectangle rectangle) {
            return rectangle;
        }
        var envelope = SpatialEnvelopeVisitor.visitGeo(geometry, SpatialEnvelopeVisitor.WrapLongitude.WRAP);
        if (envelope.isPresent()) {
            return envelope.get();
        }
        throw new IllegalArgumentException("Cannot determine envelope of bounds geometry");
    }

    protected static GeoBoundingBox asGeoBoundingBox(BytesRef boundsBytesRef) {
        return asGeoBoundingBox(asRectangle(boundsBytesRef));
    }

    protected static GeoBoundingBox asGeoBoundingBox(Rectangle rectangle) {
        return new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
    }

    protected static boolean inBounds(Point point, Rectangle bounds) {
        // TODO: consider bounds across the dateline
        return point.getX() >= bounds.getMinX()
            && point.getY() >= bounds.getMinY()
            && point.getX() <= bounds.getMaxX()
            && point.getY() <= bounds.getMaxY();
    }

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), spatialDocsValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialGridFunction other = (SpatialGridFunction) obj;
            return Objects.equals(other.children(), children()) && Objects.equals(other.spatialDocsValues, spatialDocsValues);
        }
        return false;
    }

    public boolean spatialDocsValues() {
        return spatialDocsValues;
    }

    @Override
    public final SpatialGridFunction replaceChildren(List<Expression> newChildren) {
        Expression newSpatialField = newChildren.get(0);
        Expression newParameter = newChildren.get(1);
        Expression newBounds = newChildren.size() > 2 ? newChildren.get(2) : null;

        return spatialField.equals(newSpatialField)
            && parameter.equals(newParameter)
            && (bounds == null && newBounds == null || bounds != null && bounds.equals(newBounds))
                ? this
                : replaceChildren(newSpatialField, newParameter, newBounds);
    }

    protected abstract SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds);

    public Expression spatialField() {
        return spatialField;
    }

    public Expression parameter() {
        return parameter;
    }

    public Expression bounds() {
        return bounds;
    }

    @Override
    public boolean foldable() {
        return spatialField.foldable() && parameter.foldable() && (bounds == null || bounds.foldable());
    }

    protected static void addGrids(LongBlock.Builder results, List<Long> gridIds) {
        if (gridIds.isEmpty()) {
            results.appendNull();
        } else if (gridIds.size() == 1) {
            results.appendLong(gridIds.getFirst());
        } else {
            results.beginPositionEntry();
            for (long gridId : gridIds) {
                results.appendLong(gridId);
            }
            results.endPositionEntry();
        }
    }

}
