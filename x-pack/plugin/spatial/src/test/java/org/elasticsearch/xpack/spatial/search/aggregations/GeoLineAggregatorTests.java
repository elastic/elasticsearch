/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.simplify.SimplificationErrorCalculator;
import org.elasticsearch.geometry.simplify.StreamingGeometrySimplifier;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoLineAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new SpatialPlugin(), new AggregationsPlugin());
        // return Collections.singletonList(new SpatialPlugin());
    }

    // test that missing values are ignored
    public void testMixedMissingValues() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);

        long lonLat = (((long) GeoEncodingUtils.encodeLongitude(90.0)) << 32) | GeoEncodingUtils.encodeLatitude(45.0) & 0xffffffffL;
        // input documents for testing
        // ----------------------------
        // | sort_field | value_field |
        // ----------------------------
        // | N/A | lonLat |
        // | 1 | N/A |
        // | 2 | lonLat |
        // | N/A | N/A |
        // | 4 | lonLat |
        // ----------------------------
        double[] sortValues = new double[] { -1, 1, 2, -1, 4 };
        long[] points = new long[] { lonLat, -1, lonLat, -1, lonLat };
        // expected
        long[] expectedAggPoints = new long[] { lonLat, lonLat };
        double[] expectedAggSortValues = new double[] { NumericUtils.doubleToSortableLong(2), NumericUtils.doubleToSortableLong(4) };

        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < points.length; i++) {
                List<Field> fields = new ArrayList<>();
                fields.add(new SortedDocValuesField("group_id", new BytesRef("group")));
                if (sortValues[i] != -1) {
                    fields.add(new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i])));
                }
                if (points[i] != -1) {
                    fields.add(new LatLonDocValuesField("value_field", 45.0, 90.0));
                }
                iw.addDocument(fields);
            }
        }, terms -> {
            assertThat(terms.getBuckets().size(), equalTo(1));
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
            assertThat(geoLine.length(), equalTo(2));
            assertTrue(geoLine.isComplete());
            assertArrayEquals(expectedAggPoints, geoLine.line());
            assertArrayEquals(expectedAggSortValues, geoLine.sortVals(), 0d);
        });
    }

    public void testMissingGeoPointValueField() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);

        // input
        double[] sortValues = new double[] { 1, 0, 2, 0, 3, 4, 5 };

        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < sortValues.length; i++) {
                iw.addDocument(
                    Arrays.asList(
                        new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i])),
                        new SortedDocValuesField("group_id", new BytesRef("group"))
                    )
                );
            }
        }, terms -> {
            assertThat(terms.getBuckets().size(), equalTo(1));
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
            assertThat(geoLine.length(), equalTo(0));
            assertTrue(geoLine.isComplete());
        });
    }

    public void testMissingSortField() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);

        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < 7; i++) {
                iw.addDocument(
                    Arrays.asList(
                        new LatLonDocValuesField("value_field", 45.0, 90.0),
                        new SortedDocValuesField("group_id", new BytesRef("group"))
                    )
                );
            }
        }, terms -> {
            assertThat(terms.getBuckets().size(), equalTo(1));
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
            assertThat(geoLine.length(), equalTo(0));
            assertTrue(geoLine.isComplete());
        });
    }

    public void testAscending() throws IOException {
        testAggregator(SortOrder.ASC);
    }

    public void testDescending() throws IOException {
        testAggregator(SortOrder.DESC);
    }

    public void testComplete() throws IOException {
        // max size is the same as the number of points
        testCompleteForSizeAndNumDocuments(10, 10, true);
        // max size is more than the number of points
        testCompleteForSizeAndNumDocuments(11, 10, true);
        // max size is less than the number of points
        testCompleteForSizeAndNumDocuments(9, 10, false);
    }

    public void testCompleteForSizeAndNumDocuments(int size, int numPoints, boolean complete) throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);

        Map<String, InternalGeoLine> lines = Maps.newMapWithExpectedSize(1);
        String groupOrd = "0";
        long[] points = new long[numPoints];
        double[] sortValues = new double[numPoints];
        for (int i = 0; i < numPoints; i++) {
            Point point = GeometryTestUtils.randomPoint(false);
            int encodedLat = GeoEncodingUtils.encodeLatitude(point.getLat());
            int encodedLon = GeoEncodingUtils.encodeLongitude(point.getLon());
            long lonLat = (((long) encodedLon) << 32) | encodedLat & 0xffffffffL;
            points[i] = lonLat;
            sortValues[i] = i;
        }

        int lineSize = Math.min(numPoints, size);
        // re-sort line to be ascending
        long[] linePoints = Arrays.copyOf(points, lineSize);
        double[] lineSorts = Arrays.copyOf(sortValues, lineSize);
        PathArraySorter.forOrder(SortOrder.ASC).apply(linePoints, lineSorts).sort();

        lines.put(groupOrd, new InternalGeoLine("track", linePoints, lineSorts, null, complete, true, SortOrder.ASC, size));

        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < points.length; i++) {
                int x = (int) (points[i] >> 32);
                int y = (int) points[i];
                iw.addDocument(
                    Arrays.asList(
                        new LatLonDocValuesField("value_field", GeoEncodingUtils.decodeLatitude(y), GeoEncodingUtils.decodeLongitude(x)),
                        new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i])),
                        new SortedDocValuesField("group_id", new BytesRef(groupOrd))
                    )
                );
            }
        }, terms -> {
            for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                InternalGeoLine expectedGeoLine = lines.get(bucket.getKeyAsString());
                InternalGeoLine geoLine = bucket.getAggregations().get("track");
                assertThat(geoLine.length(), equalTo(expectedGeoLine.length()));
                assertThat(geoLine.isComplete(), equalTo(expectedGeoLine.isComplete()));
                for (int i = 0; i < geoLine.sortVals().length; i++) {
                    geoLine.sortVals()[i] = NumericUtils.sortableLongToDouble((long) geoLine.sortVals()[i]);
                }
                assertArrayEquals(expectedGeoLine.sortVals(), geoLine.sortVals(), 0d);
                assertArrayEquals(expectedGeoLine.line(), geoLine.line());
            }
        });
    }

    public void testEmpty() throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);
        testCase(aggregationBuilder, iw -> {}, terms -> { assertTrue(terms.getBuckets().isEmpty()); });
    }

    public void testOnePoint() throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);
        double lon = GeoEncodingUtils.decodeLongitude(randomInt());
        double lat = GeoEncodingUtils.decodeLatitude(randomInt());
        testCase(aggregationBuilder, iw -> {
            iw.addDocument(
                Arrays.asList(
                    new LatLonDocValuesField("value_field", lat, lon),
                    new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(randomDouble())),
                    new SortedDocValuesField("group_id", new BytesRef("groupOrd"))
                )
            );
        }, terms -> {
            assertEquals(1, terms.getBuckets().size());
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
            assertNotNull(geoLine);
            Map<String, Object> geojson = geoLine.geoJSONGeometry();
            assertEquals("Point", geojson.get("type"));
            assertTrue(geojson.get("coordinates") instanceof double[]);
            double[] coordinates = (double[]) geojson.get("coordinates");
            assertEquals(2, coordinates.length);
            assertEquals(lon, coordinates[0], 1e-6);
            assertEquals(lat, coordinates[1], 1e-6);
        });
    }

    public void testGeoLine_TSDB() throws IOException {
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            assertGeoLine_TSDB(100, 100, 3, 7, sortOrder, gl -> new TimeSeriesAggregationBuilder("ts").subAggregation(gl), tsx -> {
                assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(1));
                InternalGeoLine geoLine = tsx.ts.getBuckets().get(0).getAggregations().get("track");
                assertGeoLine(sortOrder, tsx.expectedAggPoints, geoLine, true);
                assertSortValues(sortOrder, tsx.expectedAggSortValues, geoLine.sortVals());
            });
        }
    }

    public void testGeoLine_Terms_TSDB() throws IOException {
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            assertGeoLine_TSDB(80, 80, 7, 3, sortOrder, gl -> {
                var termsAggBuilder = new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                return new TimeSeriesAggregationBuilder("ts").subAggregation(termsAggBuilder);
            }, tsx -> {
                assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(1));
                StringTerms terms = tsx.ts.getBuckets().get(0).getAggregations().get("groups");
                assertThat("Number of terms buckets", terms.getBuckets().size(), equalTo(1));
                InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
                assertGeoLine(sortOrder, tsx.expectedAggPoints, geoLine, true);
                assertSortValues(sortOrder, tsx.expectedAggSortValues, geoLine.sortVals());
            });
        }
    }

    public void testGeoLine_TSDB_simplified() throws IOException {
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            assertGeoLine_TSDB(100, 10, 3, 7, sortOrder, gl -> new TimeSeriesAggregationBuilder("ts").subAggregation(gl), tsx -> {
                assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(1));
                InternalGeoLine geoLine = tsx.ts.getBuckets().get(0).getAggregations().get("track");
                assertThat("Length of GeoLine", geoLine.length(), equalTo(10));
                assertGeoLine(sortOrder, tsx.expectedAggPoints, geoLine, false);
                // TODO: We get failure on multi-segments here due to incorrect sort values for -Dtests.seed=CBF396D20D08C7C1
                assertSortValues(sortOrder, tsx.expectedAggSortValues, geoLine.sortVals());
            });
        }
    }

    public void testGeoLine_Terms_TSDB_simplified() throws IOException {
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            assertGeoLine_TSDB(80, 10, 7, 3, sortOrder, gl -> {
                var termsAggBuilder = new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                return new TimeSeriesAggregationBuilder("ts").subAggregation(termsAggBuilder);
            }, tsx -> {
                assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(1));
                StringTerms terms = tsx.ts.getBuckets().get(0).getAggregations().get("groups");
                assertThat("Number of terms buckets", terms.getBuckets().size(), equalTo(1));
                InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
                assertThat("Length of GeoLine", geoLine.length(), equalTo(10));
                assertGeoLine(sortOrder, tsx.expectedAggPoints, geoLine, false);
                assertSortValues(sortOrder, tsx.expectedAggSortValues, geoLine.sortVals());
            });
        }
    }

    private void assertGeoLine(SortOrder sortOrder, long[] expectedAggPoints, InternalGeoLine geoLine, boolean complete) {
        assertThat("Length of GeoLine '" + sortOrder + "'", geoLine.length(), equalTo(expectedAggPoints.length));
        assertThat("GeoLine is complete '" + sortOrder + "'", geoLine.isComplete(), is(complete));
        assertThat("GeoLine contents '" + sortOrder + "'", geoLine.line(), isGeoLine(expectedAggPoints));
    }

    private void assertSortValues(SortOrder sortOrder, double[] expected, double[] sortValues) {
        for (int i = 1; i < sortValues.length; i++) {
            Matcher<Double> sortMatcher = switch (sortOrder) {
                case ASC -> Matchers.greaterThan(sortValues[i - 1]);
                case DESC -> Matchers.lessThan(sortValues[i - 1]);
            };
            if (sortMatcher.matches(sortValues[i]) == false) {
                System.out.println("Failed");
            }
            assertThat("Expect ordered '" + sortOrder + "' sort values", sortValues[i], sortMatcher);
        }
        assertArrayEquals("GeoLine sort values", expected, sortValues, 0d);
    }

    private Matcher<long[]> isGeoLine(long[] line) {
        return new TestGeoLineLongArrayMatcher(line);
    }

    private static class TestGeoLineLongArrayMatcher extends BaseMatcher<long[]> {
        private final long[] expectedLine;
        private final ArrayList<String> failures = new ArrayList<>();

        private TestGeoLineLongArrayMatcher(long[] expectedLine) {
            this.expectedLine = expectedLine;
        }

        @Override
        public boolean matches(Object actualObj) {
            failures.clear();
            if (actualObj instanceof long[] actualLine) {
                if (actualLine.length != expectedLine.length) {
                    failures.add("Expected length " + expectedLine.length + " but got " + actualLine.length);
                    return false;
                }
                for (int i = 0; i < expectedLine.length; i++) {
                    Point actual = asPoint(actualLine[i]);
                    Point expected = asPoint(expectedLine[i]);
                    if (actual.equals(expected) == false) {
                        failures.add("At line position " + i + " expected " + expected + " but got " + actual);
                    }
                }
                return failures.size() == 0;
            }
            return false;
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            description.appendText("had ").appendValue(failures.size()).appendText(" failures");
            for (String failure : failures) {
                description.appendText("\n\t").appendText(failure);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(
                "Should be geoline of "
                    + expectedLine.length
                    + " starting with "
                    + asPoint(expectedLine[0])
                    + " and ending with "
                    + asPoint(expectedLine[expectedLine.length - 1])
            );
        }

        private static Point asPoint(long encoded) {
            double latitude = GeoEncodingUtils.decodeLatitude((int) (encoded & 0xffffffffL));
            double longitude = GeoEncodingUtils.decodeLongitude((int) (encoded >>> 32));
            return new Point(longitude, latitude);
        }
    }

    private record TestTSAssertionResults(MultiBucketsAggregation ts, long[] expectedAggPoints, double[] expectedAggSortValues) {}

    /**
     * Wrapper for points and sort fields that it also usable in the GeometrySimplifier library,
     * allowing us to track which points will survive geometry simplification during geo_line aggregations.
     */
    private static class TestSimplifiablePoint extends StreamingGeometrySimplifier.PointError {
        private final double sortField;
        private final long encoded;

        private TestSimplifiablePoint(int index, double x, double y, double sortField) {
            super(index, quantizeX(x), quantizeY(y));
            this.sortField = sortField;
            this.encoded = encode(x(), y());
        }

        private static double quantizeX(double x) {
            return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(x));
        }

        private static double quantizeY(double y) {
            return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(y));
        }

        private static long encode(double x, double y) {
            return (((long) GeoEncodingUtils.encodeLongitude(x)) << 32) | GeoEncodingUtils.encodeLatitude(y) & 0xffffffffL;
        }

        private static double decodeLongitude(long encoded) {
            return GeoEncodingUtils.decodeLongitude((int) (encoded >>> 32));
        }

        private static double decodeLatitude(long encoded) {
            return GeoEncodingUtils.decodeLatitude((int) (encoded & 0xffffffffL));
        }

        private void reset(int index) {
            super.reset(index, this.x(), this.y());
        }
    }

    /** Allow test to use own objects for internal use in geometry simplifier, so we can track the sort-fields together with the points */
    private static class TestLine implements Geometry {
        private final long[] encodedPoints;
        private final double[] sortValues;

        private TestLine(int length, StreamingGeometrySimplifier.PointError[] points) {
            this.encodedPoints = new long[length];
            this.sortValues = new double[length];
            for (int i = 0; i < length; i++) {
                TestSimplifiablePoint p = (TestSimplifiablePoint) points[i];
                encodedPoints[i] = p.encoded;
                sortValues[i] = p.sortField;
            }
        }

        @Override
        public ShapeType type() {
            return ShapeType.LINESTRING;
        }

        @Override
        public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
            double[] x = new double[encodedPoints.length];
            double[] y = new double[encodedPoints.length];
            for (int i = 0; i < encodedPoints.length; i++) {
                x[i] = TestSimplifiablePoint.decodeLongitude(encodedPoints[i]);
                y[i] = TestSimplifiablePoint.decodeLatitude(encodedPoints[i]);
            }
            Line line = new Line(x, y);
            return visitor.visit(line);
        }

        @Override
        public boolean isEmpty() {
            return encodedPoints.length == 0;
        }

        @Override
        public String toString() {
            return WellKnownText.toWKT(this);
        }
    }

    private static class TestGeometrySimplifierMonitor implements StreamingGeometrySimplifier.Monitor {
        private int addedCount;
        private int removedCount;

        public void reset() {
            addedCount = 0;
            removedCount = 0;
        }

        @Override
        public void pointAdded(String status, List<SimplificationErrorCalculator.PointLike> points) {
            addedCount++;
            System.out.println("Adding point " + points.get(points.size() - 1));
        }

        @Override
        public void pointRemoved(
            String status,
            List<SimplificationErrorCalculator.PointLike> points,
            SimplificationErrorCalculator.PointLike removed,
            double error,
            SimplificationErrorCalculator.PointLike previous,
            SimplificationErrorCalculator.PointLike next
        ) {
            addedCount++;
            removedCount++;
            System.out.println("Adding point " + points.get(points.size() - 1) + " and removing point " + removed);
        }

        @Override
        public void startSimplification(String description, int maxPoints) {}

        @Override
        public void endSimplification(String description, List<SimplificationErrorCalculator.PointLike> points) {}
    }

    /** Wrapping the Streaming GeometrySimplifier allowing the test to extract expected points and their sort fields after simplification */
    private static class TestGeometrySimplifier extends StreamingGeometrySimplifier<TestLine> {
        private TestGeometrySimplifier(int maxPoints, TestGeometrySimplifierMonitor monitor) {
            super("TestGeometrySimplifier", maxPoints, SimplificationErrorCalculator.TRIANGLE_AREA, monitor);
        }

        @Override
        public TestLine produce() {
            return new TestLine(this.length, this.points);
        }
    }

    private void assertGeoLine_TSDB(
        int docCount,
        int maxPoints,
        int missingPointFactor,
        int missingTimestampFactor,
        SortOrder sortOrder,
        Function<GeoLineAggregationBuilder, TimeSeriesAggregationBuilder> makeTSAggBuilder,
        Consumer<TestTSAssertionResults> verifyTSResults
    ) throws IOException {
        System.out.println("\n\nMaking test data for sort-order "+sortOrder);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("@timestamp").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(sortOrder)
            .sort(sortConfig)
            .size(maxPoints);
        TimeSeriesAggregationBuilder aggregationBuilder = makeTSAggBuilder.apply(lineAggregationBuilder);

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        double startLat = 45.0;
        double startLon = 90;
        ArrayList<GeoPoint> points = new ArrayList<>();
        ArrayList<Long> timestamps = new ArrayList<>();
        ArrayList<TestSimplifiablePoint> expectedAggPointsList = new ArrayList<>();
        // TSDB provides docs in DESC time order, so we generate the data that way to simplify assertions
        for (int i = docCount - 1; i >= 0; i--) {
            double lat = startLat + i * 0.1 + randomDoubleBetween(-0.1, 0.1, false);
            double lon = startLon + i * 0.1 + randomDoubleBetween(-0.1, 0.1, false);
            GeoPoint point = (missingPointFactor > 0 && i % missingPointFactor == 0) ? null : new GeoPoint(lat, lon);
            Long timestamp = (missingTimestampFactor > 0 && i % missingTimestampFactor == 0) ? null : startTime + 1000L * i;
            points.add(point);
            timestamps.add(timestamp);
            if (point != null && timestamp != null) {
                expectedAggPointsList.add(new TestSimplifiablePoint(expectedAggPointsList.size(), lon, lat, (double) timestamp));
            }
        }
        if (missingPointFactor > 0) {
            int deletedAtLeast = points.size() / missingPointFactor;
            assertThat(
                "Valid points less than total/" + missingPointFactor,
                expectedAggPointsList.size(),
                lessThanOrEqualTo(points.size() - deletedAtLeast)
            );
        }
        if (missingTimestampFactor > 0) {
            int deletedAtLeast = points.size() / missingTimestampFactor;
            assertThat(
                "Valid points less than total/" + missingTimestampFactor,
                expectedAggPointsList.size(),
                lessThanOrEqualTo(points.size() - deletedAtLeast)
            );
        }
        long[] expectedAggPoints;
        double[] expectedAggSortValues;
        if (maxPoints < docCount) {
            // The aggregation will simplify the line in reverse order, so we need to anticipate the same simplification in the tests
            TestGeometrySimplifierMonitor monitor = new TestGeometrySimplifierMonitor();
            var simplifier = new TestGeometrySimplifier(maxPoints, monitor);
            for (TestSimplifiablePoint p : expectedAggPointsList) {
                simplifier.consume(p);
            }
            TestLine line = simplifier.produce();
            assertThat("Simplifier added points", monitor.addedCount, equalTo(expectedAggPointsList.size()));
            assertThat("Simplifier Removed points", monitor.removedCount, equalTo(expectedAggPointsList.size() - maxPoints));
            expectedAggPoints = line.encodedPoints;
            expectedAggSortValues = line.sortValues;
        } else {
            // The aggregation will NOT simplify the line, so we should only anticipate the removal of invalid documents
            expectedAggPoints = new long[expectedAggPointsList.size()];
            expectedAggSortValues = new double[expectedAggPointsList.size()];
            for (int i = 0; i < expectedAggPoints.length; i++) {
                expectedAggPoints[i] = expectedAggPointsList.get(i).encoded;
                expectedAggSortValues[i] = expectedAggPointsList.get(i).sortField;
            }
        }
        // TSDB orders in time-descending
        if (sortOrder == SortOrder.ASC) {
            ArrayUtils.reverseSubArray(expectedAggPoints, 0, expectedAggPoints.length);
            ArrayUtils.reverseSubArray(expectedAggSortValues, 0, expectedAggSortValues.length);
        }
        System.out.println("\n\nStarting test for sort-order "+sortOrder);
        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < points.size(); i++) {
                final TimeSeriesIdFieldMapper.TimeSeriesIdBuilder builder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(null);
                builder.addString("group_id", "group");
                ArrayList<Field> fields = new ArrayList<>(
                    Arrays.asList(
                        new SortedDocValuesField("group_id", new BytesRef("group")),
                        new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, builder.build().toBytesRef())
                    )
                );
                GeoPoint point = points.get(i);
                if (point != null) {
                    fields.add(new LatLonDocValuesField("value_field", point.lat(), point.lon()));
                }
                Long timestamp = timestamps.get(i);
                if (timestamp != null) {
                    // TODO: Do we need both?
                    fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
                    fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
                }
                iw.addDocument(fields);
            }
        }, ts -> verifyTSResults.accept(new TestTSAssertionResults(ts, expectedAggPoints, expectedAggSortValues)));
        System.out.println("\n\nFinished test for sort-order " + sortOrder + "\n\n");
    }

    private void testAggregator(SortOrder sortOrder) throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(sortOrder)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("groups").field("group_id")
            .subAggregation(lineAggregationBuilder);

        int numGroups = randomIntBetween(1, 2);
        Map<String, InternalGeoLine> lines = Maps.newMapWithExpectedSize(numGroups);
        Map<Integer, long[]> indexedPoints = Maps.newMapWithExpectedSize(numGroups);
        Map<Integer, double[]> indexedSortValues = Maps.newMapWithExpectedSize(numGroups);
        for (int groupOrd = 0; groupOrd < numGroups; groupOrd++) {
            int numPoints = randomIntBetween(2, 2 * size);
            boolean complete = numPoints <= size;
            long[] points = new long[numPoints];
            double[] sortValues = new double[numPoints];
            for (int i = 0; i < numPoints; i++) {
                Point point = GeometryTestUtils.randomPoint(false);
                int encodedLat = GeoEncodingUtils.encodeLatitude(point.getLat());
                int encodedLon = GeoEncodingUtils.encodeLongitude(point.getLon());
                long lonLat = (((long) encodedLon) << 32) | encodedLat & 0xffffffffL;
                points[i] = lonLat;
                sortValues[i] = i;
            }
            int lineSize = Math.min(numPoints, size);
            // re-sort line to be ascending
            long[] linePoints = Arrays.copyOf(points, lineSize);
            double[] lineSorts = Arrays.copyOf(sortValues, lineSize);
            // When we combine sort DESC with limit (size less than number of points), we actually truncate off the beginning of the data
            if (lineSize < points.length && sortOrder == SortOrder.DESC) {
                int offset = points.length - lineSize;
                System.arraycopy(points, offset, linePoints, 0, lineSize);
                System.arraycopy(sortValues, offset, lineSorts, 0, lineSize);
            }
            PathArraySorter.forOrder(sortOrder).apply(linePoints, lineSorts).sort();

            lines.put(String.valueOf(groupOrd), new InternalGeoLine("track", linePoints, lineSorts, null, complete, true, sortOrder, size));

            for (int i = 0; i < randomIntBetween(1, numPoints); i++) {
                int idx1 = randomIntBetween(0, numPoints - 1);
                int idx2 = randomIntBetween(0, numPoints - 1);
                final long tmpPoint = points[idx1];
                points[idx1] = points[idx2];
                points[idx2] = tmpPoint;
                final double tmpSortValue = sortValues[idx1];
                sortValues[idx1] = sortValues[idx2];
                sortValues[idx2] = tmpSortValue;
            }
            indexedPoints.put(groupOrd, points);
            indexedSortValues.put(groupOrd, sortValues);
        }

        testCase(aggregationBuilder, iw -> {
            for (int group = 0; group < numGroups; group++) {
                long[] points = indexedPoints.get(group);
                double[] sortValues = indexedSortValues.get(group);
                for (int i = 0; i < points.length; i++) {
                    int x = (int) (points[i] >> 32);
                    int y = (int) points[i];
                    iw.addDocument(
                        Arrays.asList(
                            new LatLonDocValuesField(
                                "value_field",
                                GeoEncodingUtils.decodeLatitude(y),
                                GeoEncodingUtils.decodeLongitude(x)
                            ),
                            new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i])),
                            new SortedDocValuesField("group_id", new BytesRef(String.valueOf(group)))
                        )
                    );
                }
            }
        }, terms -> {
            for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                InternalGeoLine expectedGeoLine = lines.get(bucket.getKeyAsString());
                InternalGeoLine geoLine = bucket.getAggregations().get("track");
                assertThat(geoLine.length(), equalTo(expectedGeoLine.length()));
                assertThat(geoLine.isComplete(), equalTo(expectedGeoLine.isComplete()));
                for (int i = 0; i < geoLine.sortVals().length; i++) {
                    geoLine.sortVals()[i] = NumericUtils.sortableLongToDouble((long) geoLine.sortVals()[i]);
                }
                assertArrayEquals(expectedGeoLine.sortVals(), geoLine.sortVals(), 0d);
                assertArrayEquals(expectedGeoLine.line(), geoLine.line());
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <A extends MultiBucketsAggregation, C extends Aggregator> void testCase(
        AggregationBuilder aggregationBuilder,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<A> verify
    ) throws IOException {

        IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
        boolean timeSeries = aggregationBuilder.isInSortOrderExecutionRequired();
        if (timeSeries) {
            Sort sort = new Sort(
                new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
                new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
            );
            config.setIndexSort(sort);
        }

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            ArrayList<MappedFieldType> fieldTypes = new ArrayList<>();
            if (timeSeries) {
                fieldTypes.add(TimeSeriesIdFieldMapper.FIELD_TYPE);
                fieldTypes.add(new DateFieldMapper.DateFieldType("@timestamp"));
            }

            TimeSeriesParams.MetricType metricType = timeSeries ? TimeSeriesParams.MetricType.POSITION : null;
            fieldTypes.add(new GeoPointFieldMapper.GeoPointFieldType("value_field", metricType));
            fieldTypes.add(new KeywordFieldMapper.KeywordFieldType("group_id", false, true, Collections.emptyMap()));
            fieldTypes.add(new NumberFieldMapper.NumberFieldType("sort_field", NumberFieldMapper.NumberType.LONG));
            AggTestConfig aggTestConfig = new AggTestConfig(aggregationBuilder, fieldTypes.toArray(new MappedFieldType[0]));

            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                A terms = (A) searchAndReduce(indexSearcher, aggTestConfig);
                verify.accept(terms);
            }
        }
    }
}
