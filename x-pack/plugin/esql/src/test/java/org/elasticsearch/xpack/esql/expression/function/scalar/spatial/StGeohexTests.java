/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.esql.common.spatial.H3CartesianUtil;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class StGeohexTests extends SpatialGridFunctionTestCase {
    public StGeohexTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Other geo grid functions use the same type-specific license requirement as the spatial aggregations, but geohex is licensed
     * more strictly, at platinum for all field types.
     */
    public static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        return License.OperationMode.PLATINUM;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        addTestCaseSuppliers(
            suppliers,
            new DataType[] { GEO_POINT, GEO_SHAPE },
            GEOHEX,
            StGeohexTests::valueOf,
            StGeohexTests::boundedValueOf
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static Object valueOf(BytesRef wkb, int precision) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return StGeohex.unboundedGrid.calculateGridId(point, precision);
        }
        try {
            return SpatialGridFunction.foldMultiValue(StGeohex.computeGeohexCells(wkb, precision, null));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
        }
    }

    private static Object boundedValueOf(BytesRef wkb, int precision, GeoBoundingBox bbox) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            StGeohex.GeoHexBoundedGrid bounds = new StGeohex.GeoHexBoundedGrid.Factory(precision, bbox).get(null);
            long gridId = bounds.calculateGridId(point);
            return gridId < 0 ? null : gridId;
        }
        try {
            return SpatialGridFunction.foldMultiValue(StGeohex.computeGeohexCells(wkb, precision, bbox));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression bounds = args.size() > 2 ? args.get(2) : null;
        return new StGeohex(source, args.get(0), args.get(1), bounds);
    }

    public void testInvalidPrecision() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(-1, StGeohexTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohex_grid precision of -1. Must be between 0 and 15."));
        ex = expectThrows(IllegalArgumentException.class, () -> process(H3.MAX_H3_RES + 1, StGeohexTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohex_grid precision of 16. Must be between 0 and 15."));
    }

    // --- Polar-cell and noChild regression tests ---
    // Adapted from GeoHexTilerTests in the spatial module. Each test compares
    // computeGeohexCells (the recursive H3 tree traversal) against a brute-force
    // enumeration that checks every res-N cell individually using the leaf-level
    // intersection test. The brute-force is independent of the tree traversal and
    // will catch false negatives from missing noChild visits or over-pruning near
    // the poles.

    /**
     * A polygon almost entirely within the south-pole res-0 cell.
     * Without the polar forced-recurse fix the intermediate res-0 cell's bounding
     * box check can misclassify the cell as disjoint in the equirectangular projection,
     * causing the south pole cell to be silently dropped.
     * Adapted from {@code GeoHexTilerTests.testTroublesomeShapeAlmostWithinSouthPoleCell_UnboundedGeoShapeCellValues}.
     */
    public void testShapeAlmostWithinSouthPoleCell_Unbounded() throws Exception {
        String polygon = """
            POLYGON((1.7481549674935762E-110 -90.0, 180.0 -90.0, 180.0 -75.113250736563,
            1.7481549674935762E-110 -75.113250736563, 1.7481549674935762E-110 -90.0))""";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        assertGeohexCellsMatchBruteForce(geometry, 0);
    }

    /**
     * A polygon whose bounding box clips the south-pole res-1 cell.
     * Without the polar forced-recurse fix the intermediate cell can be pruned before
     * the exact leaf-level test is reached.
     * Adapted from {@code GeoHexTilerTests.testTroublesomeShapeAlmostWithinSouthPole_BoundedGeoShapeCellValues}.
     */
    public void testShapeAlmostWithinSouthPoleCell_Unbounded_Precision1() throws Exception {
        String polygon = """
            POLYGON((180.0 -90.0, 180.0 -73.80002960532788, 1.401298464324817E-45 -73.80002960532788,
            1.401298464324817E-45 -90.0, 180.0 -90.0))""";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        assertGeohexCellsMatchBruteForce(geometry, 1);
    }

    /**
     * A polygon whose bounding box clips the north-pole res-1 cell.
     * Without the polar forced-recurse fix the north-polar intermediate cell can be
     * pruned before reaching the leaf-level check.
     * Adapted from {@code GeoHexTilerTests.testTroublesomeShapeAlmostWithinNorthPoleCell_UnboundedGeoShapeCellValues}.
     */
    public void testShapeAlmostWithinNorthPoleCell_Unbounded() throws Exception {
        String polygon = """
            POLYGON((36.98661841690625 69.44049730644747, 180.0 69.44049730644747,
            180.0 90.0, 36.98661841690625 90.0, 36.98661841690625 69.44049730644747))""";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        assertGeohexCellsMatchBruteForce(geometry, 1);
    }

    /**
     * A bounding-box shape partially overlapping the north-polar res-1 band.
     * The polar forced-recurse guard must prevent premature pruning of the polar
     * intermediate cell even when the equirectangular bbox appears disjoint.
     * Adapted from {@code GeoHexTilerTests.testTroublesomePolarCellLevel1_UnboundedGeoShapeCellValues}.
     */
    public void testShapeOverlappingPolarBand_Unbounded() throws Exception {
        String bbox = "BBOX (-84.24596376729815, 43.36113427778119, 90.0, 83.51476833522361)";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, bbox);
        assertGeohexCellsMatchBruteForce(geometry, 1);
    }

    /**
     * A bounded query with two points near the antimeridian.
     * Without the noChild loop, cells whose H3 parent spans the antimeridian can be
     * silently missed when the parent's bounding box doesn't intersect the shape.
     * Adapted from {@code GeoHexTilerTests.testTroublesomeCellLevel2_BoundedGeoShapeCellValues}.
     */
    public void testShapeNearAntimeridian_Bounded() throws Exception {
        String wkt = """
            GEOMETRYCOLLECTION (
              GEOMETRYCOLLECTION (
                POINT(-170 0),
                POINT (-178.5 0)
              )
            )
            """;
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(4E-4, 179.999), new GeoPoint(-4E-4, -179.999));
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, wkt);
        assertGeohexCellsMatchBruteForce(geometry, 2, bbox);
    }

    /**
     * A polygon near the north pole with a tight bounding box.
     * Exercises both the polar forced-recurse guard and noChild visits near high latitudes.
     * Adapted from {@code GeoHexTilerTests.testTroublesomeCellLevel4_BoundedGeoShapeCellValues}.
     */
    public void testShapeNearNorthPole_Bounded() throws Exception {
        String polygon = "POLYGON ((150.0 70.0, 150.0 85.91811374669217, 168.77544806565834 85.91811374669217, 150.0 70.0))";
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(86.17678739494652, 172.21916569181505), new GeoPoint(83.01600086049713, 179));
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        assertGeohexCellsMatchBruteForce(geometry, 4, bbox);
    }

    // ---- Brute-force helpers ----

    /**
     * Asserts that {@link StGeohex#computeGeohexCells} (recursive tree traversal) produces
     * the same set of H3 cells as a brute-force enumeration that checks every res-{@code precision}
     * cell individually using the leaf-level intersection test. The brute-force is independent
     * of the H3 tree traversal and catches false negatives from missing noChild visits or
     * incorrect pruning in polar regions.
     */
    private static void assertGeohexCellsMatchBruteForce(Geometry geometry, int precision) throws IOException {
        BytesRef wkb = GEO.asWkb(geometry);
        List<Long> recursive = new ArrayList<>(StGeohex.computeGeohexCells(wkb, precision, null));
        List<Long> brute = bruteForceGeohexCells(wkb, precision, null);
        Collections.sort(recursive);
        Collections.sort(brute);
        assertEquals("ST_GEOHEX recursive and brute-force results differ at precision " + precision, brute, recursive);
    }

    private static void assertGeohexCellsMatchBruteForce(Geometry geometry, int precision, GeoBoundingBox bbox) throws IOException {
        BytesRef wkb = GEO.asWkb(geometry);
        List<Long> recursive = new ArrayList<>(StGeohex.computeGeohexCells(wkb, precision, bbox));
        List<Long> brute = bruteForceGeohexCells(wkb, precision, bbox);
        Collections.sort(recursive);
        Collections.sort(brute);
        assertEquals(
            "ST_GEOHEX recursive and brute-force results differ at precision " + precision + " with bbox " + bbox,
            brute,
            recursive
        );
    }

    /**
     * Brute-force reference: iterates every H3 cell at {@code precision} from the res-0 roots
     * and checks each one individually using the exact leaf-level intersection test
     * ({@link H3CartesianUtil#getLatLonGeometry}). This is independent of the recursive tree
     * traversal used in production and therefore catches pruning bugs (noChild, polar forcing).
     */
    private static List<Long> bruteForceGeohexCells(BytesRef wkb, int precision, GeoBoundingBox bbox) throws IOException {
        GeoShapeDocValues shape = GeoShapeDocValues.from(wkb, SpatialGridFunction.GEO_SHAPE_INDEXER);
        GeoHexBoundedPredicate predicate = bbox == null ? null : new GeoHexBoundedPredicate(bbox);
        List<Long> cells = new ArrayList<>();
        bruteForceRecurse(shape, H3.getLongRes0Cells(), precision, predicate, cells);
        return cells;
    }

    private static void bruteForceRecurse(
        GeoShapeDocValues shape,
        long[] h3s,
        int targetRes,
        GeoHexBoundedPredicate predicate,
        List<Long> cells
    ) throws IOException {
        for (long h3 : h3s) {
            if (H3.getResolution(h3) == targetRes) {
                if (predicate == null || predicate.validHex(h3)) {
                    if (shape.intersects(LatLonGeometry.create(H3CartesianUtil.getLatLonGeometry(h3)))) {
                        cells.add(h3);
                    }
                }
            } else {
                bruteForceRecurse(shape, H3.h3ToChildren(h3), targetRes, predicate, cells);
            }
        }
    }

    // --- Dateline-crossing cell tests ---

    /**
     * Verifies that {@link StGeohex#toBounds} never produces a polygon whose vertices are more
     * than 180° away from the cell centre longitude. The invariant is: for every returned vertex
     * longitude {@code lon}, {@code |lon - centerLon| <= 180}.
     *
     * <p>This ensures map clients receive a compact, contiguous polygon rather than one that
     * appears to wrap the entire globe (which would happen if a vertex near −175° were left
     * unchanged when the centre is at +175°, giving a polygon that spans 350° the wrong way).
     * Dateline-crossing cells will have some vertex longitudes outside [−180, 180] after the fix,
     * which is intentional — the ESQL response path (WKB → WKT) skips coordinate validation.
     *
     * <p>We check every resolution-0 and resolution-1 cell and assert:
     * <ol>
     *   <li>At least one cell actually needs adjustment (exercises the fix).</li>
     *   <li>No returned polygon vertex is more than 180° from the cell centre.</li>
     * </ol>
     */
    public void testToBoundsNeverStraddlesDateline() {
        int crossingCells = 0;
        for (long res0 : H3.getLongRes0Cells()) {
            crossingCells += assertToBoundsVerticesWithin180OfCenter(res0);
            for (long res1 : H3.h3ToChildren(res0)) {
                crossingCells += assertToBoundsVerticesWithin180OfCenter(res1);
            }
        }
        // Sanity check: at least some cells must need adjustment so the fix is actually exercised.
        assertThat("Expected at least one cell needing dateline normalisation among res-0 and res-1 cells", crossingCells, greaterThan(0));
    }

    /**
     * Returns 1 if the raw H3 cell boundary has any vertex more than 180° from the cell centre
     * (i.e. the normalisation fix was needed for this cell), and 0 otherwise. In both cases
     * asserts that {@link StGeohex#toBounds} returns a polygon where every vertex is within ±180°
     * of the cell centre longitude.
     */
    private static int assertToBoundsVerticesWithin180OfCenter(long h3) {
        LatLng center = H3.h3ToLatLng(h3);
        double centerLon = center.getLonDeg();

        // Count whether the raw boundary has any vertex that needs normalisation.
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        boolean rawNeedsNorm = false;
        for (int i = 0; i < boundary.numPoints(); i++) {
            double lon = boundary.getLatLon(i).getLonDeg();
            if (Math.abs(lon - centerLon) > 180.0) {
                rawNeedsNorm = true;
                break;
            }
        }

        // After the fix, every vertex must be within ±180° of the cell centre.
        // We use NOOP validation because dateline-crossing cells may produce longitudes outside
        // [−180, 180], which is intentional — the ESQL response path (wkbToWkt) skips validation.
        BytesRef wkb = StGeohex.toBounds(h3);
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
        assertThat("toBounds must return a Polygon for cell " + H3.h3ToString(h3), geometry instanceof Polygon, not(false));
        LinearRing ring = ((Polygon) geometry).getPolygon();
        for (int i = 0; i < ring.length(); i++) {
            double lon = ring.getX(i);
            assertTrue(
                "Cell " + H3.h3ToString(h3) + " (centerLon=" + centerLon + "): vertex lon " + lon + " is more than 180° from centre",
                Math.abs(lon - centerLon) <= 180.0
            );
        }

        return rawNeedsNorm ? 1 : 0;
    }

    /**
     * Spot-checks a res-0 cell known to cross the antimeridian (dateline).
     * Such a cell has at least one raw vertex whose longitude is more than 180° from the
     * cell centre; without the fix those vertices produce a polygon that wraps the globe.
     *
     * <p>After the fix, every vertex in the returned polygon must be within ±180° of the centre.
     */
    public void testToBoundsKnownDatelineCrossingCell() {
        // Find a res-0 cell where at least one raw vertex is more than 180° from the centre.
        long crossingCell = -1;
        for (long res0 : H3.getLongRes0Cells()) {
            double centerLon = H3.h3ToLatLng(res0).getLonDeg();
            CellBoundary boundary = H3.h3ToGeoBoundary(res0);
            for (int i = 0; i < boundary.numPoints(); i++) {
                double lon = boundary.getLatLon(i).getLonDeg();
                if (Math.abs(lon - centerLon) > 180.0) {
                    crossingCell = res0;
                    break;
                }
            }
            if (crossingCell != -1) break;
        }
        assumeTrue("No res-0 dateline-crossing cell found (unexpected)", crossingCell != -1);

        double centerLon = H3.h3ToLatLng(crossingCell).getLonDeg();

        BytesRef wkb = StGeohex.toBounds(crossingCell);
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
        assertThat("toBounds must return a Polygon", geometry instanceof Polygon, not(false));
        LinearRing ring = ((Polygon) geometry).getPolygon();
        for (int i = 0; i < ring.length(); i++) {
            double lon = ring.getX(i);
            assertTrue(
                "Dateline-crossing cell "
                    + H3.h3ToString(crossingCell)
                    + " (centerLon="
                    + centerLon
                    + "): vertex lon "
                    + lon
                    + " must be within ±180° of centre after fix",
                Math.abs(lon - centerLon) <= 180.0
            );
        }
    }
}
