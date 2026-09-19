/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.common.spatial.GeoHexGridTiler;
import org.elasticsearch.xpack.esql.common.spatial.GeoShapeDocValues;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Calculates the geohex of geo_point or geo_shape geometries.
 * For geo_shape, all intersecting H3 cells are returned as multi-values.
 */
public class StGeohex extends SpatialGridFunction implements EvaluatorMapper, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StGeohex", StGeohex::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StGeohex.class).ternary(StGeohex::new).name("st_geohex");

    /**
     * When checking grid cells with bounds, we need to check if the cell is valid (intersects with the bounds).
     * This uses GeoHexBoundedPredicate to check if the cell is valid.
     */
    protected static class GeoHexBoundedGrid implements BoundedGrid {
        private final int precision;
        private final GeoHexBoundedPredicate bounds;

        GeoHexBoundedGrid(int precision, GeoBoundingBox bbox) {
            this.precision = checkPrecisionRange(precision);
            this.bounds = new GeoHexBoundedPredicate(bbox);
        }

        public long calculateGridId(Point point) {
            // For points, filtering the point is as good as filtering the tile
            long geohex = H3.geoToH3(point.getLat(), point.getLon(), precision);
            if (bounds.validHex(geohex)) {
                return geohex;
            }
            // H3 explicitly requires the highest bit to be zero, freeing up all negative numbers as invalid ids. See H3.isValidHex()
            return -1L;
        }

        @Override
        public int precision() {
            return precision;
        }

        protected static class Factory {
            private final int precision;
            private final GeoBoundingBox bbox;

            Factory(int precision, GeoBoundingBox bbox) {
                this.precision = checkPrecisionRange(precision);
                this.bbox = bbox;
            }

            public GeoHexBoundedGrid get(DriverContext context) {
                return new GeoHexBoundedGrid(precision, bbox);
            }
        }
    }

    /**
     * For unbounded grids, we don't need to check if the tile is valid,
     * just calculate the encoded long intersecting the point at that precision.
     */
    public static final UnboundedGrid unboundedGrid = (point, precision) -> H3.geoToH3(
        point.getLat(),
        point.getLon(),
        checkPrecisionRange(precision)
    );

    private static int checkPrecisionRange(int precision) {
        if (precision < 0 || precision > H3.MAX_H3_RES) {
            throw new IllegalArgumentException(
                "Invalid geohex_grid precision of " + precision + ". Must be between 0 and " + H3.MAX_H3_RES + "."
            );
        }
        return precision;
    }

    @Override
    protected BlockLoaderFunctionConfig.GeoGrid blockLoaderConfig(int precision, @Nullable GeoBoundingBox bounds) {
        if (precision < 0 || precision > H3.MAX_H3_RES) {
            return null;
        }
        Supplier<BlockLoaderFunctionConfig.GeoGridEncoder> encoders;
        if (bounds == null) {
            encoders = () -> (lon, lat) -> H3.geoToH3(lat, lon, precision);
        } else {
            // The bounded grid keeps scratch state, so build one per encoder; it returns -1 for a point outside the bounds
            encoders = () -> {
                GeoHexBoundedGrid grid = new GeoHexBoundedGrid(precision, bounds);
                return (lon, lat) -> grid.calculateGridId(new Point(lon, lat));
            };
        }
        BlockLoaderFunctionConfig.GeoGridShapeTilerFactory shapeTilers = shapeTilers(encoders, () -> {
            GeoHexGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, bounds);
            return (shape, onTruncation) -> tiler.cells(shape, MAX_GRID_CELLS, onTruncation);
        });
        return new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOHEX,
            precision,
            bounds,
            encoders,
            shapeTilers
        );
    }

    @FunctionInfo(
        returnType = "geohex",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        briefSummary = "Calculates the geohex (H3 cell-id) of the supplied geo_point or geo_shape at the specified precision.",
        description = """
            Calculates the `geohex`, the H3 cell-id, of the supplied `geo_point` or `geo_shape` at the specified precision.
            For `geo_shape` inputs, all intersecting H3 cells are returned as multi-values.
            The result is long encoded.
            Use [`TO_STRING`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_string.md)
            to convert the result to a string,
            [`TO_LONG`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_long.md)
            to convert it to a `long`, or
            [`TO_GEOSHAPE`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_geoshape.md)
            to calculate the `geo_shape` bounding geometry.

            These functions are related to the [`geo_grid` query](/reference/query-languages/query-dsl/query-dsl-geo-grid-query.md)
            and the [`geohex_grid` aggregation](/reference/aggregations/search-aggregations-bucket-geohexgrid-aggregation.md).""",
        examples = @Example(file = "spatial-grid", tag = "st_geohex-grid"),
        depthOffset = 1  // So this appears as a subsection of spatial grid functions
    )
    public StGeohex(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape" },
            description = "Expression of type `geo_point` or `geo_shape`. If `null`, the function returns `null`."
                + " For `geo_shape` inputs all intersecting H3 cells are returned as multi-values."
        ) Expression field,
        @Param(name = "precision", type = { "integer" }, hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT), description = """
            Expression of type `integer`. If `null`, the function returns `null`.
            Valid values are between [0 and 15](https://h3geo.org/docs/core-library/restable/).""") Expression precision,
        @Param(name = "bounds", type = { "geo_shape" }, hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT), description = """
            Optional bounds to filter the grid tiles, a `geo_shape` of type `BBOX`. Use
            [`ST_ENVELOPE`](/reference/query-languages/esql/functions-operators/spatial-functions/st_envelope.md)
            if the `geo_shape` is of any other type.""", optional = true) Expression bounds
    ) {
        this(source, field, precision, bounds, false);
    }

    private StGeohex(Source source, Expression field, Expression precision, Expression bounds, boolean spatialDocValues) {
        super(source, field, precision, bounds, spatialDocValues);
    }

    private StGeohex(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return state.isAllowedByLicense(License.OperationMode.PLATINUM);
    }

    @Override
    public SpatialGridFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean docValues = this.spatialDocValues || useDocValues;
        return new StGeohex(source(), spatialField, parameter, bounds, docValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return GEOHEX;
    }

    @Override
    protected SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds) {
        return new StGeohex(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohex::new, spatialField, parameter, bounds);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (parameter().foldable() == false) {
            throw new IllegalArgumentException("precision must be foldable");
        }
        if (bounds != null) {
            if (bounds.foldable() == false) {
                throw new IllegalArgumentException("bounds must be foldable");
            }
            Object boundsValue = bounds.fold(toEvaluator.foldCtx());
            if (boundsValue == null) {
                return ConstantEvaluators.CONSTANT_NULL_FACTORY;
            }
            GeoBoundingBox bbox = asGeoBoundingBox(boundsValue);
            int precision = (int) parameter.fold(toEvaluator.foldCtx());
            GeoHexBoundedGrid.Factory bounds = new GeoHexBoundedGrid.Factory(precision, bbox);
            Source evalSource = source();
            Function<DriverContext, GeoShapeCellsComputer> shapeTilerFactory = ctx -> {
                Warnings w = ctx.createOnlyWarnings(evalSource);
                GeoHexGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, bbox);
                return wkb -> tiler.cells(GeoShapeDocValues.from(wkb, GEO_SHAPE_INDEXER), MAX_GRID_CELLS, w::registerWarning);
            };
            return spatialDocValues
                ? new StGeohexFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                    source(),
                    toEvaluator.apply(spatialField()),
                    bounds::get
                )
                : new StGeohexFromFieldAndLiteralAndLiteralEvaluator.Factory(
                    source(),
                    toEvaluator.apply(spatialField),
                    bounds::get,
                    shapeTilerFactory
                );
        } else {
            int precision = checkPrecisionRange((int) parameter.fold(toEvaluator.foldCtx()));
            Source evalSource = source();
            Function<DriverContext, GeoShapeCellsComputer> shapeTilerFactory = ctx -> {
                Warnings w = ctx.createOnlyWarnings(evalSource);
                GeoHexGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, null);
                return wkb -> tiler.cells(GeoShapeDocValues.from(wkb, GEO_SHAPE_INDEXER), MAX_GRID_CELLS, w::registerWarning);
            };
            return spatialDocValues
                ? new StGeohexFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                : new StGeohexFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision, shapeTilerFactory);
        }
    }

    @Override
    public Object fold(FoldContext ctx) {
        var wkb = (BytesRef) spatialField().fold(ctx);
        if (wkb == null) {
            return null;
        }
        int precision = checkPrecisionRange((int) parameter().fold(ctx));
        try {
            if (bounds() == null) {
                Geometry geometry = GEO.wkbToGeometry(wkb);
                if (geometry instanceof Point point) {
                    return unboundedGrid.calculateGridId(point, precision);
                }
                return foldMultiValue(computeGeohexCells(wkb, precision, null, foldWarningConsumer()));
            } else {
                Object boundsValue = bounds().fold(ctx);
                if (boundsValue == null) {
                    return null;
                }
                GeoBoundingBox bbox = asGeoBoundingBox(boundsValue);
                Geometry geometry = GEO.wkbToGeometry(wkb);
                if (geometry instanceof Point point) {
                    GeoHexBoundedGrid bounds = new GeoHexBoundedGrid(precision, bbox);
                    long gridId = bounds.calculateGridId(point);
                    return gridId < 0 ? null : gridId;
                }
                return foldMultiValue(computeGeohexCells(wkb, precision, bbox, foldWarningConsumer()));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
        }
    }

    @Evaluator(extraName = "FromFieldAndLiteral")
    static void fromFieldAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        BytesRefBlock wkbBlock,
        @Fixed int precision,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoShapeCellsComputer shapeTiler
    ) {
        fromWKB(results, p, wkbBlock, precision, unboundedGrid, shapeTiler);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteral(LongBlock.Builder results, @Position int p, LongBlock encoded, @Fixed int precision) {
        fromEncodedLong(results, p, encoded, precision, unboundedGrid);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral")
    static void fromFieldAndLiteralAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        BytesRefBlock in,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoHexBoundedGrid bounds,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoShapeCellsComputer shapeTiler
    ) {
        fromWKB(results, p, in, bounds, shapeTiler);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        LongBlock encoded,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoHexBoundedGrid bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }

    public static BytesRef toBounds(long gridId) {
        LatLng center = H3.h3ToLatLng(gridId);
        return fromCellBoundary(H3.h3ToGeoBoundary(gridId), center.getLonDeg());
    }

    /**
     * Converts an H3 {@link CellBoundary} to a WKB-encoded {@link Polygon}.
     *
     * <p>H3 cells near the antimeridian (dateline) can have vertices whose longitudes span both
     * sides of ±180°. Such polygons render incorrectly in map clients (e.g. Kibana) because the
     * straight line drawn between, say, +175° and −175° crosses the entire map rather than the
     * short arc across the dateline.
     *
     * <p>To fix this, each vertex longitude is adjusted so that it lies within ±180° of the cell
     * centre longitude. Concretely: if {@code lon - centerLon > 180} the vertex is shifted west by
     * 360°; if {@code lon - centerLon < -180} it is shifted east by 360°. This keeps all vertices
     * in a contiguous range centred on {@code centerLon} and may produce longitudes outside
     * [−180, 180] (e.g. 190° or −190°) for cells that straddle the antimeridian. That is
     * intentional — the ESQL response path converts WKB to WKT without coordinate validation, so
     * the extended values reach the map client as-is.
     *
     * @param cell      the H3 cell boundary
     * @param centerLon the longitude of the H3 cell centre, used as the reference for normalisation
     */
    private static BytesRef fromCellBoundary(CellBoundary cell, double centerLon) {
        double[] x = new double[cell.numPoints() + 1];
        double[] y = new double[cell.numPoints() + 1];
        for (int i = 0; i < cell.numPoints(); i++) {
            LatLng vertex = cell.getLatLon(i);
            double lon = vertex.getLonDeg();
            // Bring the vertex within ±180° of the cell centre. This correctly handles cells
            // near the antimeridian (dateline) regardless of which side the centre is on, and
            // does not disturb cells near the prime meridian.
            if (lon - centerLon > 180.0) {
                lon -= 360.0;
            } else if (lon - centerLon < -180.0) {
                lon += 360.0;
            }
            x[i] = lon;
            y[i] = vertex.getLatDeg();
        }
        x[cell.numPoints()] = x[0];
        y[cell.numPoints()] = y[0];
        LinearRing ring = new LinearRing(x, y);
        Polygon polygon = new Polygon(ring);
        return SpatialCoordinateTypes.GEO.asWkb(polygon);
    }

    // ---- Geohex cell computation for geo_shape ----

    /**
     * Computes all H3 cells at the given precision that intersect the WKB-encoded geometry,
     * truncating at {@link SpatialGridFunction#MAX_GRID_CELLS} and calling {@code onTruncation}
     * with a warning message when the limit is reached.
     * <p>
     * The cells are found by {@link GeoHexGridTiler}, the ES|QL copy of the tiler behind the {@code geohex_grid}
     * aggregation, so the result matches that aggregation.
     * </p>
     * The fold path emits warnings via HTTP response headers using {@link SpatialGridFunction#foldWarningConsumer()};
     * the evaluator path passes {@code warnings::registerWarning} so the user sees a driver-context warning.
     */
    static List<Long> computeGeohexCells(BytesRef wkb, int precision, GeoBoundingBox bbox, Consumer<String> onTruncation)
        throws IOException {
        return computeGeohexCells(GeoShapeDocValues.from(wkb, GEO_SHAPE_INDEXER), precision, bbox, onTruncation);
    }

    /**
     * Same as {@link #computeGeohexCells(BytesRef, int, GeoBoundingBox, Consumer)} but on a triangle tree that is already
     * available. Builds a fresh {@link GeoHexGridTiler}, so callers computing cells for many shapes should instead keep
     * one tiler per thread, as the evaluator and the block loader do.
     */
    static List<Long> computeGeohexCells(GeoShapeDocValues shape, int precision, GeoBoundingBox bbox, Consumer<String> onTruncation)
        throws IOException {
        return GeoHexGridTiler.makeGridTiler(precision, bbox).cells(shape, MAX_GRID_CELLS, onTruncation);
    }
}
