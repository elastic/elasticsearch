/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.geo.LatLonGeometry;
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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeotile.fromRectangle;

/**
 * Calculates the geohash of geo_point or geo_shape geometries.
 * For geo_shape, all intersecting geohash cells are returned as multi-values.
 */
public class StGeohash extends SpatialGridFunction implements EvaluatorMapper, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohash",
        StGeohash::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StGeohash.class).ternary(StGeohash::new).name("st_geohash");

    /**
     * When checking grid cells with bounds, we need to check if the cell is valid (intersects with the bounds).
     * This uses GeoHashBoundedPredicate to check if the grid cell is valid.
     */
    protected static class GeoHashBoundedGrid implements BoundedGrid {
        private final int precision;
        private final GeoHashBoundedPredicate bounds;

        private GeoHashBoundedGrid(int precision, GeoBoundingBox bbox) {
            this.precision = checkPrecisionRange(precision);
            this.bounds = new GeoHashBoundedPredicate(precision, bbox);
        }

        public long calculateGridId(Point point) {
            String geohash = Geohash.stringEncode(point.getX(), point.getY(), precision);
            if (bounds.validHash(geohash)) {
                return Geohash.longEncode(geohash);
            }
            // Geohash uses the lowest 4 bites for precision, and if all four are set, we get 15, which is invalid since the
            // max precision allowed is 12. This allows us to return -1 to indicate invalid geohashes (since all bits are set to 1).
            return -1;
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

            public GeoHashBoundedGrid get(DriverContext context) {
                return new GeoHashBoundedGrid(precision, bbox);
            }
        }
    }

    /**
     * For unbounded grids, we don't need to check if the grid cell is valid,
     * just calculate the encoded long intersecting the point at that precision.
     */
    public static final UnboundedGrid unboundedGrid = (point, precision) -> Geohash.longEncode(
        point.getX(),
        point.getY(),
        checkPrecisionRange(precision)
    );

    private static int checkPrecisionRange(int precision) {
        if (precision < 1 || precision > Geohash.PRECISION) {
            throw new IllegalArgumentException(
                "Invalid geohash_grid precision of " + precision + ". Must be between 1 and " + Geohash.PRECISION + "."
            );
        }
        return precision;
    }

    @FunctionInfo(
        returnType = "geohash",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        briefSummary = "Calculates the geohash of the supplied geo_point or geo_shape at the specified precision.",
        description = """
            Calculates the `geohash` of the supplied `geo_point` or `geo_shape` at the specified precision.
            For `geo_shape` inputs, all intersecting geohash cells are returned as multi-values.
            The result is long encoded.
            Use [`TO_STRING`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_string.md)
            to convert the result to a string,
            [`TO_LONG`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_long.md)
            to convert it to a `long`, or
            [`TO_GEOSHAPE`](/reference/query-languages/esql/functions-operators/type-conversion-functions/to_geoshape.md)
            to calculate the `geo_shape` bounding geometry.

            These functions are related to the [`geo_grid` query](/reference/query-languages/query-dsl/query-dsl-geo-grid-query.md)
            and the [`geohash_grid` aggregation](/reference/aggregations/search-aggregations-bucket-geohashgrid-aggregation.md).""",
        examples = @Example(file = "spatial-grid", tag = "st_geohash-grid"),
        depthOffset = 1  // So this appears as a subsection of spatial grid functions
    )
    public StGeohash(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape" },
            description = "Expression of type `geo_point` or `geo_shape`. If `null`, the function returns `null`."
                + " For `geo_shape` inputs all intersecting geohash cells are returned as multi-values."
        ) Expression field,
        @Param(name = "precision", type = { "integer" }, hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT), description = """
            Expression of type `integer`. If `null`, the function returns `null`.
            Valid values are between [1 and 12](https://en.wikipedia.org/wiki/Geohash).""") Expression precision,
        @Param(name = "bounds", type = { "geo_shape" }, hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT), description = """
            Optional bounds to filter the grid tiles, a `geo_shape` of type `BBOX`. Use
            [`ST_ENVELOPE`](/reference/query-languages/esql/functions-operators/spatial-functions/st_envelope.md)
            if the `geo_shape` is of any other type.""", optional = true) Expression bounds
    ) {
        this(source, field, precision, bounds, false);
    }

    private StGeohash(Source source, Expression field, Expression precision, Expression bounds, boolean spatialDocValues) {
        super(source, field, precision, bounds, spatialDocValues);
    }

    private StGeohash(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public SpatialGridFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean docValues = this.spatialDocValues || useDocValues;
        return new StGeohash(source(), spatialField, parameter, bounds, docValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return GEOHASH;
    }

    @Override
    protected SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds) {
        return new StGeohash(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohash::new, spatialField, parameter, bounds);
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
            GeoHashBoundedGrid.Factory bounds = new GeoHashBoundedGrid.Factory(precision, bbox);
            GeoShapeCellsComputer shapeTiler = wkb -> computeGeohashCells(wkb, precision, bbox);
            return spatialDocValues
                ? new StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                    source(),
                    toEvaluator.apply(spatialField()),
                    bounds::get
                )
                : new StGeohashFromFieldAndLiteralAndLiteralEvaluator.Factory(
                    source(),
                    toEvaluator.apply(spatialField),
                    bounds::get,
                    shapeTiler
                );
        } else {
            int precision = checkPrecisionRange((int) parameter.fold(toEvaluator.foldCtx()));
            GeoShapeCellsComputer shapeTiler = wkb -> computeGeohashCells(wkb, precision, null);
            return spatialDocValues
                ? new StGeohashFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                : new StGeohashFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision, shapeTiler);
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
                return foldMultiValue(computeGeohashCells(wkb, precision, null));
            } else {
                Object boundsValue = bounds().fold(ctx);
                if (boundsValue == null) {
                    return null;
                }
                GeoBoundingBox bbox = asGeoBoundingBox(boundsValue);
                Geometry geometry = GEO.wkbToGeometry(wkb);
                if (geometry instanceof Point point) {
                    GeoHashBoundedGrid bounds = new GeoHashBoundedGrid(precision, bbox);
                    long gridId = bounds.calculateGridId(point);
                    return gridId < 0 ? null : gridId;
                }
                return foldMultiValue(computeGeohashCells(wkb, precision, bbox));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohash for geo_shape", e);
        }
    }

    @Evaluator(extraName = "FromFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        BytesRefBlock wkbBlock,
        @Fixed int precision,
        @Fixed(includeInToString = false) GeoShapeCellsComputer shapeTiler
    ) {
        fromWKB(results, p, wkbBlock, precision, unboundedGrid, shapeTiler);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteral(LongBlock.Builder results, @Position int p, LongBlock encoded, @Fixed int precision) {
        fromEncodedLong(results, p, encoded, precision, unboundedGrid);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        BytesRefBlock in,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoHashBoundedGrid bounds,
        @Fixed(includeInToString = false) GeoShapeCellsComputer shapeTiler
    ) {
        fromWKB(results, p, in, bounds, shapeTiler);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        @Position int p,
        LongBlock encoded,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoHashBoundedGrid bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }

    public static BytesRef toBounds(long gridId) {
        return fromRectangle(Geohash.toBoundingBox(Geohash.stringEncode(gridId)));
    }

    // ---- Geohash cell computation for geo_shape ----

    /**
     * Computes all geohash cells at the given precision that intersect the WKB-encoded geometry.
     * Optionally filtered by a bounding box.
     * <p>
     * The algorithm and the brute-force vs. rasterization heuristic ({@code dX * dY <= 32 * precision})
     * are adapted from {@code GeoHashGridTiler.setValues} in the spatial module.
     * Both thresholds should be reviewed together if either is changed.
     */
    static List<Long> computeGeohashCells(BytesRef wkb, int precision, GeoBoundingBox bbox) throws IOException {
        GeoShapeDocValues shape = GeoShapeDocValues.from(wkb, GEO_SHAPE_INDEXER);
        GeoHashBoundedPredicate predicate = (bbox == null || bbox.isUnbounded()) ? null : new GeoHashBoundedPredicate(precision, bbox);
        List<Long> cells = new ArrayList<>();
        long dX = (long) Math.ceil((shape.maxLon - shape.minLon) / Geohash.lonWidthInDegrees(precision));
        long dY = (long) Math.ceil((shape.maxLat - shape.minLat) / Geohash.latHeightInDegrees(precision));
        if (dX * dY <= 32L * precision) {
            geohashBruteForceScan(shape, precision, predicate, cells);
        } else {
            rasterizeGeohash(shape, "", precision, predicate, cells);
        }
        return cells;
    }

    /**
     * Iterates all geohash cells in the shape bounding box west-to-east and south-to-north,
     * adding those that intersect the shape (and pass the optional bounds filter).
     * Adapted from {@code GeoHashGridTiler.setValuesByBruteForceScan} in the spatial module.
     */
    private static void geohashBruteForceScan(GeoShapeDocValues shape, int precision, GeoHashBoundedPredicate predicate, List<Long> cells)
        throws IOException {
        final String stop = Geohash.stringEncode(shape.maxLon, shape.maxLat, precision);
        String firstInRow = null;
        String lastInRow = null;
        do {
            lastInRow = (lastInRow == null)
                ? Geohash.stringEncode(shape.maxLon, shape.minLat, precision)
                : Geohash.getNeighbor(lastInRow, precision, 0, 1);
            String current = null;
            do {
                if (current == null) {
                    firstInRow = (firstInRow == null)
                        ? Geohash.stringEncode(shape.minLon, shape.minLat, precision)
                        : Geohash.getNeighbor(firstInRow, precision, 0, 1);
                    current = firstInRow;
                } else {
                    current = Geohash.getNeighbor(current, precision, 1, 0);
                }
                if (geohashCellIntersectsShape(shape, current, predicate)) {
                    if (cells.size() >= SpatialGridFunction.MAX_GRID_CELLS) {
                        throw new IllegalArgumentException(
                            "ST_GEOHASH generated more than " + SpatialGridFunction.MAX_GRID_CELLS + " grid cells"
                        );
                    }
                    cells.add(Geohash.longEncode(current));
                }
            } while (current.equals(lastInRow) == false);
        } while (lastInRow.equals(stop) == false);
    }

    /**
     * Recursively enumerates geohash sub-cells, descending until target precision is reached,
     * adding cells that intersect the shape.
     * Adapted from {@code GeoHashGridTiler.setValuesByRasterization} in the spatial module.
     */
    private static void rasterizeGeohash(
        GeoShapeDocValues shape,
        String hash,
        int precision,
        GeoHashBoundedPredicate predicate,
        List<Long> cells
    ) throws IOException {
        for (String sub : Geohash.getSubGeohashes(hash)) {
            if (geohashCellIntersectsShape(shape, sub, predicate)) {
                if (sub.length() == precision) {
                    if (cells.size() >= SpatialGridFunction.MAX_GRID_CELLS) {
                        throw new IllegalArgumentException(
                            "ST_GEOHASH generated more than " + SpatialGridFunction.MAX_GRID_CELLS + " grid cells"
                        );
                    }
                    cells.add(Geohash.longEncode(sub));
                } else {
                    rasterizeGeohash(shape, sub, precision, predicate, cells);
                }
            }
        }
    }

    /**
     * Tests whether the given geohash cell intersects the shape (and passes the optional bounds filter).
     * Replaces {@code GeoHashGridTiler.relateTile} using {@link GeoShapeDocValues#intersects} with a
     * Lucene {@link LatLonGeometry} rectangle instead of encoded-integer coordinates.
     */
    private static boolean geohashCellIntersectsShape(GeoShapeDocValues shape, String hash, GeoHashBoundedPredicate predicate)
        throws IOException {
        if (predicate != null && predicate.validHash(hash) == false) {
            return false;
        }
        org.elasticsearch.geometry.Rectangle esRect = Geohash.toBoundingBox(hash);
        org.apache.lucene.geo.Rectangle luceneRect = new org.apache.lucene.geo.Rectangle(
            esRect.getMinLat(),
            esRect.getMaxLat(),
            esRect.getMinLon(),
            esRect.getMaxLon()
        );
        return shape.intersects(LatLonGeometry.create(luceneRect));
    }
}
