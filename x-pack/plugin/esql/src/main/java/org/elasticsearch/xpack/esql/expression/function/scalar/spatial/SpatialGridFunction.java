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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.common.spatial.GeoShapeDocValues;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
public abstract class SpatialGridFunction extends SpatialDocValuesFunction
    implements
        OptionalArgument,
        LicenseAware,
        BlockLoaderExpression {
    /**
     * Maximum number of grid cells that a single geo_shape value may intersect. When a shape intersects more
     * cells than this limit the result is silently truncated to a partial list; the evaluator additionally
     * emits an ES|QL warning so the user knows the output is incomplete. Mirrors the
     * 10 000-document convention used elsewhere in Elasticsearch to give operators a familiar threshold.
     * <p>
     * For {@code geo_point} and {@code geo_shape} fields with doc values the function is fused into field loading, see
     * {@link #tryPushToFieldLoading}, so the geometry is never materialised and any {@code STATS} on top runs on
     * the loaded cell ids directly.
     * </p>
     * <p>
     * TODO: for the common pattern {@code BY ST_GEOHEX(shape, precision)} the query planner could rewrite
     *       the scalar function to a dedicated geo-grid aggregator (like the spatial plugin's
     *       {@code GeoHexGridAggregationBuilder}) that processes one document at a time and accumulates
     *       per-bucket counters rather than materialising all cells for an entire page at once.
     *       This would eliminate the per-page memory explosion without needing a hard cell limit.
     * </p>
     * <p>
     * TODO: for {@code WHERE ST_GEOHEX(shape, precision) == cell_id} the predicate could be pushed down
     *       to Lucene as a geo-grid query (analogous to the Query DSL {@code geo_grid} query), avoiding
     *       the need to enumerate cells at all and dramatically reducing both CPU and memory usage.
     * </p>
     */
    public static final int MAX_GRID_CELLS = 10_000;
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

    /**
     * Fuses this function into the loading of a {@code geo_point} or {@code geo_shape} field so the cells are computed
     * straight from the doc values and the geometry is never read from {@code _source} nor materialised as a block. See
     * {@link BlockLoaderExpression} for the general mechanism. Only grids over a mapped field with doc values, a constant
     * in-range precision and, if present, constant envelope bounds qualify. Null or invalid bounds keep using the
     * evaluator so that it reports them. Shapes are tiled with the same algorithm as the evaluator, including the
     * {@link #MAX_GRID_CELLS} truncation and its warning; the one difference is that the doc value holds the union of a
     * document's shapes, so a cell shared by two shapes of a multi-valued field is loaded once instead of twice.
     */
    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        if (spatialField instanceof FieldAttribute field
            && (field.dataType() == GEO_POINT || field.dataType() == GEO_SHAPE)
            && parameter instanceof Literal literal
            && literal.value() instanceof Integer precision
            && stats.hasDocValues(field.fieldName())) {
            GeoBoundingBox bbox = null;
            if (bounds != null) {
                if (bounds instanceof Literal boundsLiteral && boundsLiteral.value() instanceof BytesRef wkb) {
                    try {
                        bbox = asGeoBoundingBox(wkb);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                } else {
                    return null;
                }
            }
            BlockLoaderFunctionConfig.GeoGrid config = blockLoaderConfig(precision, bbox);
            if (config != null) {
                return new PushedBlockLoaderExpression(field, config);
            }
        }
        return null;
    }

    /**
     * The block loader configuration for this grid type at the given precision, restricted to {@code bounds} when not
     * null, or {@code null} if the precision is out of range, in which case the evaluator is left to report the error.
     */
    protected abstract BlockLoaderFunctionConfig.GeoGrid blockLoaderConfig(int precision, @Nullable GeoBoundingBox bounds);

    /** Computes the cells of a shape for {@link #shapeTilers}, reporting truncation through the consumer. */
    @FunctionalInterface
    protected interface ShapeCells {
        List<Long> compute(GeoShapeDocValues shape, Consumer<String> onTruncation) throws IOException;
    }

    /**
     * Builds the shape tiler factory for a block loader config so that fused loading of a {@code geo_shape} field behaves
     * exactly like evaluating the function on the loaded shape: a single point is encoded with the point encoder rather
     * than tiled, which matters at cell boundaries, and everything else runs the evaluator's tiling algorithm on the
     * stored triangle tree, including truncation and its warning.
     */
    protected static BlockLoaderFunctionConfig.GeoGridShapeTilerFactory shapeTilers(
        Supplier<BlockLoaderFunctionConfig.GeoGridEncoder> encoders,
        Supplier<ShapeCells> shapeCellsSupplier
    ) {
        return warnings -> {
            Consumer<String> onTruncation = warnings == null ? message -> {} : warnings::registerWarning;
            BlockLoaderFunctionConfig.GeoGridEncoder encoder = encoders.get();
            // Created per reader, like the encoder, since the geohex tiler keeps scratch state
            ShapeCells shapeCells = shapeCellsSupplier.get();
            return encoded -> {
                GeoShapeDocValues shape = GeoShapeDocValues.fromDocValue(encoded);
                if (shape.isSinglePoint()) {
                    long cell = encoder.encode(shape.centroidLon(), shape.centroidLat());
                    return cell < 0 ? List.of() : List.of(cell);
                }
                return shapeCells.compute(shape, onTruncation);
            };
        };
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

    /**
     * Creates a {@link Consumer}{@code <String>} for use in the constant-folding path that emits
     * truncation warnings to HTTP response headers (the plan-time warning channel). This produces
     * the same formatted string as
     * {@link org.elasticsearch.compute.operator.Warnings#registerWarning(String)} so that test
     * assertions expressed as {@code withWarning(...)} cover both the fold and the evaluator paths.
     */
    protected Consumer<String> foldWarningConsumer() {
        Source src = source();
        return msg -> HeaderWarning.addWarning("Line " + src.lineNumber() + ":" + src.columnNumber() + " [" + src.text() + "]: " + msg);
    }

    /**
     * Converts a {@link List}{@code <Long>} of cell IDs to the fold-result format: {@code null} for empty,
     * a single {@link Long} for one cell, or a {@link List}{@code <Long>} for multiple cells.
     */
    protected static Object foldMultiValue(List<Long> cells) {
        if (cells.isEmpty()) {
            return null;
        } else if (cells.size() == 1) {
            return cells.get(0);
        } else {
            return cells;
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
        List<Long> compute(BytesRef wkb) throws IOException;
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
                addGrids(results, cellsComputer.compute(wkb));
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
                gridIds.addAll(cellsComputer.compute(wkb));
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
                addGrids(results, cellsComputer.compute(wkb));
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
                gridIds.addAll(cellsComputer.compute(wkb));
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
