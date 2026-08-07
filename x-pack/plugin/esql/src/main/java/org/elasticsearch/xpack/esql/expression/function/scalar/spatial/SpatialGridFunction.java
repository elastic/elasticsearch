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
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Spatial functions that take one spatial argument, one parameter and one optional bounds can inherit from this class.
 * Obvious choices are: StGeohash, StGeotile and StGeohex.
 */
public abstract class SpatialGridFunction extends SpatialDocValuesFunction implements OptionalArgument, LicenseAware {
    protected final Expression spatialField;
    protected final Expression parameter;
    protected final Expression bounds;

    protected SpatialGridFunction(
        Source source,
        Expression spatialField,
        Expression parameter,
        Expression bounds,
        boolean spatialDocValues
    ) {
        super(
            source,
            bounds == null ? Arrays.asList(spatialField, parameter) : Arrays.asList(spatialField, parameter, bounds),
            spatialDocValues
        );
        this.spatialField = spatialField;
        this.parameter = parameter;
        this.bounds = bounds;
    }

    protected SpatialGridFunction(StreamInput in, boolean spatialDocValues) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            spatialDocValues
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(spatialField);
        out.writeNamedWriteable(parameter);
        out.writeOptionalNamedWriteable(bounds);
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return switch (spatialField().dataType()) {
            case GEO_SHAPE, CARTESIAN_SHAPE -> state.isAllowedByLicense(License.OperationMode.PLATINUM);
            default -> true;
        };
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isGeoPointOrShape(spatialField(), sourceText());
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isType(parameter, t -> t.equals(INTEGER), sourceText(), SECOND, INTEGER.typeName());
        if (resolution.unresolved()) {
            return resolution;
        }

        if (bounds() != null) {
            resolution = isGeoshape(bounds(), sourceText());
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    protected static final GeoShapeIndexer GEO_SHAPE_INDEXER = new GeoShapeIndexer(Orientation.CCW, "esql-geo-grid");

    protected static Expression.TypeResolution isGeoPoint(Expression e, String operationName) {
        return isType(e, t -> t.equals(GEO_POINT), operationName, FIRST, GEO_POINT.typeName());
    }

    protected static Expression.TypeResolution isGeoPointOrShape(Expression e, String operationName) {
        return isType(
            e,
            t -> t.equals(GEO_POINT) || t.equals(GEO_SHAPE),
            operationName,
            FIRST,
            GEO_POINT.typeName() + " or " + GEO_SHAPE.typeName()
        );
    }

    protected static Expression.TypeResolution isGeoshape(Expression e, String operationName) {
        return isType(e, t -> t.equals(GEO_SHAPE), operationName, THIRD, GEO_SHAPE.typeName());
    }

    protected static Rectangle asRectangle(BytesRef boundsBytesRef) {
        var geometry = GEO.wkbToGeometry(boundsBytesRef);
        if (geometry instanceof Rectangle rectangle) {
            return rectangle;
        }
        throw new IllegalArgumentException("Bounds geometry type '" + geometry.getClass().getSimpleName() + "' is not an envelope");
    }

    protected static GeoBoundingBox asGeoBoundingBox(Object bounds) {
        if (bounds instanceof BytesRef boundsBytesRef) {
            return asGeoBoundingBox(asRectangle(boundsBytesRef));
        }
        throw new IllegalArgumentException("Cannot determine envelope of bounds geometry of type " + bounds.getClass().getSimpleName());
    }

    protected static GeoBoundingBox asGeoBoundingBox(Rectangle rectangle) {
        return new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
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

    @Override
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

    protected static void addGrids(LongBlock.Builder results, long[] gridIds) {
        if (gridIds.length == 0) {
            results.appendNull();
        } else if (gridIds.length == 1) {
            results.appendLong(gridIds[0]);
        } else {
            results.beginPositionEntry();
            for (long gridId : gridIds) {
                results.appendLong(gridId);
            }
            results.endPositionEntry();
        }
    }

    /**
     * Converts a {@code long[]} of cell IDs to the fold-result format: {@code null} for empty,
     * a single {@link Long} for one cell, or a {@link List}{@code <Long>} for multiple cells.
     */
    protected static Object foldMultiValue(long[] cells) {
        if (cells.length == 0) {
            return null;
        } else if (cells.length == 1) {
            return cells[0];
        } else {
            List<Long> list = new ArrayList<>(cells.length);
            for (long cell : cells) {
                list.add(cell);
            }
            return list;
        }
    }

    /** Public for use in integration tests */
    public interface UnboundedGrid {
        long calculateGridId(Point point, int precision);
    }

    protected interface BoundedGrid {
        long calculateGridId(Point point);

        int precision();
    }

    /**
     * Computes grid cell IDs for a geo_shape WKB; replaces the spatial plugin's GeoGridTiler.
     * Implementations should return all cell IDs that intersect the given shape.
     */
    @FunctionalInterface
    protected interface GeoShapeCellsComputer {
        long[] compute(BytesRef wkb) throws IOException;
    }

    protected static void fromWKB(
        LongBlock.Builder results,
        int position,
        BytesRefBlock wkbBlock,
        int precision,
        UnboundedGrid unboundedGrid,
        GeoShapeCellsComputer cellsComputer
    ) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
            return;
        }
        final BytesRef scratch = new BytesRef();
        final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
        if (valueCount == 1) {
            addGridIdsFromWkb(results, wkbBlock.getBytesRef(firstValueIndex, scratch), precision, unboundedGrid, cellsComputer);
        } else {
            // multi-valued field — flatten all grid ids from all values
            List<Long> gridIds = new ArrayList<>();
            for (int i = 0; i < valueCount; i++) {
                appendGridIds(gridIds, wkbBlock.getBytesRef(firstValueIndex + i, scratch), precision, unboundedGrid, cellsComputer);
            }
            addGrids(results, gridIds);
        }
    }

    private static void addGridIdsFromWkb(
        LongBlock.Builder results,
        BytesRef wkb,
        int precision,
        UnboundedGrid unboundedGrid,
        GeoShapeCellsComputer cellsComputer
    ) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            results.appendLong(unboundedGrid.calculateGridId(point, precision));
        } else {
            try {
                long[] cells = cellsComputer.compute(wkb);
                addGrids(results, cells);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to compute grid cells for geo_shape", e);
            }
        }
    }

    private static void appendGridIds(
        List<Long> gridIds,
        BytesRef wkb,
        int precision,
        UnboundedGrid unboundedGrid,
        GeoShapeCellsComputer cellsComputer
    ) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            gridIds.add(unboundedGrid.calculateGridId(point, precision));
        } else {
            try {
                for (long cell : cellsComputer.compute(wkb)) {
                    gridIds.add(cell);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to compute grid cells for geo_shape", e);
            }
        }
    }

    protected static void fromEncodedLong(
        LongBlock.Builder results,
        int position,
        LongBlock encoded,
        int precision,
        UnboundedGrid unboundedGrid
    ) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendLong(unboundedGrid.calculateGridId(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(unboundedGrid.calculateGridId(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    protected static void fromWKB(
        LongBlock.Builder results,
        int position,
        BytesRefBlock wkbBlock,
        BoundedGrid bounds,
        GeoShapeCellsComputer cellsComputer
    ) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
            return;
        }
        final BytesRef scratch = new BytesRef();
        final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
        if (valueCount == 1) {
            addBoundedGridIdsFromWkb(results, wkbBlock.getBytesRef(firstValueIndex, scratch), bounds, cellsComputer);
        } else {
            var gridIds = new ArrayList<Long>();
            for (int i = 0; i < valueCount; i++) {
                appendBoundedGridIds(gridIds, wkbBlock.getBytesRef(firstValueIndex + i, scratch), bounds, cellsComputer);
            }
            addGrids(results, gridIds);
        }
    }

    private static void addBoundedGridIdsFromWkb(
        LongBlock.Builder results,
        BytesRef wkb,
        BoundedGrid bounds,
        GeoShapeCellsComputer cellsComputer
    ) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            long grid = bounds.calculateGridId(point);
            if (grid < 0) {
                results.appendNull();
            } else {
                results.appendLong(grid);
            }
        } else {
            // bounded cellsComputer already filters out-of-bounds cells
            try {
                long[] cells = cellsComputer.compute(wkb);
                addGrids(results, cells);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to compute grid cells for geo_shape", e);
            }
        }
    }

    private static void appendBoundedGridIds(List<Long> gridIds, BytesRef wkb, BoundedGrid bounds, GeoShapeCellsComputer cellsComputer) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            long grid = bounds.calculateGridId(point);
            if (grid >= 0) {
                gridIds.add(grid);
            }
        } else {
            try {
                for (long cell : cellsComputer.compute(wkb)) {
                    gridIds.add(cell);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to compute grid cells for geo_shape", e);
            }
        }
    }

    protected static void fromEncodedLong(LongBlock.Builder results, int position, LongBlock encoded, BoundedGrid bounds) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = bounds.calculateGridId(GEO.longAsPoint(encoded.getLong(firstValueIndex)));
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = bounds.calculateGridId(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)));
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }
}
