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
import org.elasticsearch.common.geo.GeoPoint;
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
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.common.spatial.H3CartesianUtil;
import org.elasticsearch.xpack.esql.common.spatial.H3SphericalUtil;
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
import java.util.ArrayList;
import java.util.List;

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

        private GeoHexBoundedGrid(int precision, GeoBoundingBox bbox) {
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
            GeoShapeCellsComputer shapeTiler = wkb -> computeGeohexCells(wkb, precision, bbox);
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
                    shapeTiler
                );
        } else {
            int precision = checkPrecisionRange((int) parameter.fold(toEvaluator.foldCtx()));
            GeoShapeCellsComputer shapeTiler = wkb -> computeGeohexCells(wkb, precision, null);
            return spatialDocValues
                ? new StGeohexFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                : new StGeohexFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision, shapeTiler);
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
                return foldMultiValue(computeGeohexCells(wkb, precision, null));
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
                return foldMultiValue(computeGeohexCells(wkb, precision, bbox));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
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
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) GeoHexBoundedGrid bounds,
        @Fixed(includeInToString = false) GeoShapeCellsComputer shapeTiler
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
        return fromCellBoundary(H3.h3ToGeoBoundary(gridId));
    }

    private static BytesRef fromCellBoundary(CellBoundary cell) {
        double[] x = new double[cell.numPoints() + 1];
        double[] y = new double[cell.numPoints() + 1];
        for (int i = 0; i < cell.numPoints(); i++) {
            LatLng vertex = cell.getLatLon(i);
            x[i] = vertex.getLonDeg();
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
     * Computes all H3 cells at the given precision that intersect the WKB-encoded geometry.
     * Optionally filtered by a bounding box.
     * <p>
     * The recursive H3-tree descent strategy is adapted from {@code GeoHexGridTiler.setValuesByRecursion}
     * in the spatial module. The bounding-box pre-check per level replaces the {@code GeoHexVisitor}
     * approach (which depends on {@code H3CartesianUtil} from the spatial module) with
     * {@link GeoShapeDocValues#intersects} over a Lucene {@link LatLonGeometry} bounding rectangle.
     * At leaf level an exact hexagon polygon intersection is performed.
     */
    static List<Long> computeGeohexCells(BytesRef wkb, int precision, GeoBoundingBox bbox) throws IOException {
        GeoShapeDocValues shape = GeoShapeDocValues.from(wkb, GEO_SHAPE_INDEXER);
        GeoHexBoundedPredicate predicate = bbox == null ? null : new GeoHexBoundedPredicate(bbox);
        List<Long> cells = new ArrayList<>();
        // Scratch bbox is reused across recursion levels to avoid per-cell allocation
        GeoBoundingBox scratch = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        for (long res0cell : H3.getLongRes0Cells()) {
            recursiveGeohex(shape, res0cell, precision, predicate, cells, scratch);
        }
        return cells;
    }

    /**
     * Recursively descends the H3 hierarchy, adding cells that intersect the shape.
     * Adapted from {@code GeoHexGridTiler.setValuesByRecursion} in the spatial module.
     *
     * <p>Two subtleties from the original are preserved here:
     * <ol>
     *   <li><b>Polar forced-recurse</b>: near the poles the equirectangular projection distorts
     *       H3 cell shapes enough that the bounding-box pre-check becomes unreliable at intermediate
     *       resolutions. When an intermediate cell's bbox touches the polar band for that resolution
     *       we skip the bbox check and always recurse, matching the {@code QUERY_CROSSES} return in
     *       {@code UnboundedGeoHexGridTiler.relateTile}.</li>
     *   <li><b>noChild (non-intersecting-children) loop</b>: H3 children at resolution N+1 can
     *       physically extend beyond their resolution-N parent's area. A target-resolution cell C
     *       may be an H3 child of parent Q but overlap a different intermediate cell P. If Q's bbox
     *       does not intersect the shape, Q's branch is pruned and C is never visited through Q.
     *       After recursing all H3 children of a non-disjoint cell P, we therefore also check every
     *       noChild of P — a cell in the next resolution that intersects P but whose H3 parent is
     *       Q ≠ P — and recurse it only when Q's bbox is disjoint from the shape (i.e. Q was or
     *       would be pruned). See {@code H3.h3ToNoChildrenIntersecting} and the comment in
     *       {@code GeoHexGridTiler.setValuesByRecursion}.</li>
     * </ol>
     */
    private static void recursiveGeohex(
        GeoShapeDocValues shape,
        long h3,
        int targetRes,
        GeoHexBoundedPredicate predicate,
        List<Long> cells,
        GeoBoundingBox scratch
    ) throws IOException {
        int res = H3.getResolution(h3);
        if (res == targetRes) {
            // At the target resolution: apply the exact hexagon intersection test.
            H3SphericalUtil.computeGeoBounds(h3, scratch);
            if (geohexBboxIntersectsShape(shape, scratch) == false) {
                return;
            }
            if (predicate == null || predicate.validHex(h3)) {
                if (h3CellIntersectsShape(shape, h3)) {
                    if (cells.size() >= SpatialGridFunction.MAX_GRID_CELLS) {
                        throw new IllegalArgumentException(
                            "ST_GEOHEX generated more than " + SpatialGridFunction.MAX_GRID_CELLS + " grid cells"
                        );
                    }
                    cells.add(h3);
                }
            }
        } else {
            // At intermediate resolutions: use the bbox as a fast pruning check.
            // Near the poles the equirectangular projection distorts cell shapes, so skip the bbox check
            // for polar-band cells and always recurse (equivalent to QUERY_CROSSES in the original tiler).
            H3SphericalUtil.computeGeoBounds(h3, scratch);
            boolean inPolarBand = scratch.top() > H3CartesianUtil.getNorthPolarBound(res)
                || scratch.bottom() < H3CartesianUtil.getSouthPolarBound(res);
            if (inPolarBand == false && geohexBboxIntersectsShape(shape, scratch) == false) {
                return;
            }
            // Recurse all H3 children of this cell.
            for (long child : H3.h3ToChildren(h3)) {
                recursiveGeohex(shape, child, targetRes, predicate, cells, scratch);
            }
            // H3 cells at the next resolution can physically extend beyond their H3 parent's area.
            // Visit each noChild (a next-resolution cell that intersects this cell but has a different
            // H3 parent) only when that H3 parent's bbox is disjoint from the shape — meaning it was
            // or would be pruned, so the noChild will never be reached through its own parent's branch.
            for (long noChild : H3.h3ToNoChildrenIntersecting(h3)) {
                long noChildParent = H3.h3ToParent(noChild);
                H3SphericalUtil.computeGeoBounds(noChildParent, scratch);
                if (geohexBboxIntersectsShape(shape, scratch) == false) {
                    recursiveGeohex(shape, noChild, targetRes, predicate, cells, scratch);
                }
            }
        }
    }

    /**
     * Tests whether the H3 bounding box (computed via {@link H3SphericalUtil}) overlaps the shape,
     * used as a fast pre-check before the exact hexagon polygon test.
     * Dateline-crossing cells use a conservative full-longitude rectangle.
     */
    private static boolean geohexBboxIntersectsShape(GeoShapeDocValues shape, GeoBoundingBox hexBbox) throws IOException {
        if (hexBbox.top() < shape.minLat || hexBbox.bottom() > shape.maxLat) {
            return false;
        }
        org.apache.lucene.geo.Rectangle luceneRect;
        if (hexBbox.left() > hexBbox.right()) {
            // Crosses dateline — use the full longitude range as conservative approximation
            luceneRect = new org.apache.lucene.geo.Rectangle(hexBbox.bottom(), hexBbox.top(), -180, 180);
        } else {
            luceneRect = new org.apache.lucene.geo.Rectangle(hexBbox.bottom(), hexBbox.top(), hexBbox.left(), hexBbox.right());
        }
        return shape.intersects(LatLonGeometry.create(luceneRect));
    }

    /**
     * Tests whether the shape intersects the H3 cell exactly.
     * Uses {@link H3CartesianUtil#getLatLonGeometry(long)} which is adapted from {@code H3CartesianGeometry}
     * in the spatial module. It handles coordinate quantization, polar cells, and dateline-crossing cells
     * correctly. In particular it handles larger H3 cells due to great-circle-arc vs. straight-line projection differences.
     */
    private static boolean h3CellIntersectsShape(GeoShapeDocValues shape, long h3) throws IOException {
        return shape.intersects(LatLonGeometry.create(H3CartesianUtil.getLatLonGeometry(h3)));
    }
}
