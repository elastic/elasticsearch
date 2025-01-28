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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.Math.min;
import static org.elasticsearch.index.IndexMode.TIME_SERIES;
import static org.elasticsearch.index.mapper.TimeSeriesParams.MetricType.POSITION;
import static org.hamcrest.CoreMatchers.containsString;
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

        int lineSize = min(numPoints, size);
        // re-sort line to be ascending
        long[] linePoints = Arrays.copyOf(points, lineSize);
        double[] lineSorts = Arrays.copyOf(sortValues, lineSize);
        PathArraySorter.forOrder(SortOrder.ASC).apply(linePoints, lineSorts).sort();

        lines.put(groupOrd, new InternalGeoLine("track", linePoints, lineSorts, null, complete, true, SortOrder.ASC, size, false, false));

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

    public void testMissingSort() throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        GeoLineAggregationBuilder glAggBuilder = new GeoLineAggregationBuilder("track").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .size(size);

        // geo_line requires 'sort'
        var e = expectThrows(IllegalArgumentException.class, () -> testCase(glAggBuilder, iw -> {}, result -> {}));
        assertThat("geo_line", e.getMessage(), containsString("missing field [sort]"));

        // geo_line requires 'sort' inside terms agg
        TermsAggregationBuilder termsAggBuilder = new TermsAggregationBuilder("groups").field("group_id").subAggregation(glAggBuilder);
        e = expectThrows(IllegalArgumentException.class, () -> testCase(termsAggBuilder, iw -> {}, result -> {}));
        assertThat("terms->geo_line", e.getMessage(), containsString("missing field [sort]"));

        // geo_line does not require 'sort' inside time-series agg
        TimeSeriesAggregationBuilder tsAggBuilder = new TimeSeriesAggregationBuilder("ts").subAggregation(glAggBuilder);
        testCase(tsAggBuilder, iw -> {}, result -> {
            assertThat(result.getName(), equalTo("ts"));
            assertThat(result.getBuckets().size(), equalTo(0));
        });
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
        for (boolean useTimestamp : new boolean[] { true, false }) {
            for (int g : new int[] { 1, 3 }) {
                for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
                    var testConfig = new TestConfig(100, 100, 3, 7, g, sortOrder, useTimestamp);
                    Function<GeoLineAggregationBuilder, AggregationBuilder> aggBuilderFunc = useTimestamp
                        ? gl -> new TimeSeriesAggregationBuilder("ts").subAggregation(gl)
                        : gl -> new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                    assertGeoLine_TSDB(testConfig, aggBuilderFunc, tsx -> {
                        assertThat("Number of groups matches number of buckets", tsx.ts.getBuckets().size(), equalTo(tsx.groups.length));
                        assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                        for (int i = 0; i < tsx.groups.length; i++) {
                            InternalGeoLine geoLine = tsx.ts.getBuckets().get(i).getAggregations().get("track");
                            assertGeoLine(sortOrder, tsx.groups[i], geoLine, tsx, true);
                        }
                    });
                }
            }
        }
    }

    public void testGeoLine_Terms_TSDB() throws IOException {
        for (int g : new int[] { 1, 3 }) {
            for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
                var testConfig = new TestConfig(80, 80, 7, 3, g, sortOrder, true);
                assertGeoLine_TSDB(testConfig, gl -> {
                    var termsAggBuilder = new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                    return new TimeSeriesAggregationBuilder("ts").subAggregation(termsAggBuilder);
                }, tsx -> {
                    assertThat("Number of groups matches number of buckets", tsx.ts.getBuckets().size(), equalTo(tsx.groups.length));
                    assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                    for (int i = 0; i < tsx.groups.length; i++) {
                        assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                        StringTerms terms = tsx.ts.getBuckets().get(i).getAggregations().get("groups");
                        assertThat("Number of terms buckets", terms.getBuckets().size(), equalTo(1));
                        InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
                        assertGeoLine(sortOrder, tsx.groups[i], geoLine, tsx, true);
                    }
                });
            }
        }
    }

    public void testGeoLine_TSDB_simplified() throws IOException {
        for (boolean useTimestamp : new boolean[] { true, false }) {
            for (int g : new int[] { 1, 3 }) {
                for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
                    var testConfig = new TestConfig(100, 10, 3, 7, g, sortOrder, useTimestamp);
                    Function<GeoLineAggregationBuilder, AggregationBuilder> aggBuilderFunc = useTimestamp
                        ? gl -> new TimeSeriesAggregationBuilder("ts").subAggregation(gl)
                        : gl -> new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                    assertGeoLine_TSDB(testConfig, aggBuilderFunc, tsx -> {
                        assertThat("Number of groups matches number of buckets", tsx.ts.getBuckets().size(), equalTo(tsx.groups.length));
                        assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                        for (int i = 0; i < tsx.groups.length; i++) {
                            InternalGeoLine geoLine = tsx.ts.getBuckets().get(i).getAggregations().get("track");
                            assertGeoLine(sortOrder, tsx.groups[i], geoLine, tsx, false);
                        }
                    });
                }
            }
        }
    }

    public void testGeoLine_Terms_TSDB_simplified() throws IOException {
        for (int g : new int[] { 1, 3 }) {
            for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
                var testConfig = new TestConfig(80, 10, 7, 3, g, sortOrder, true);
                assertGeoLine_TSDB(testConfig, gl -> {
                    var termsAggBuilder = new TermsAggregationBuilder("groups").field("group_id").subAggregation(gl);
                    return new TimeSeriesAggregationBuilder("ts").subAggregation(termsAggBuilder);
                }, tsx -> {
                    assertThat("Number of groups matches number of buckets", tsx.ts.getBuckets().size(), equalTo(tsx.groups.length));
                    assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                    for (int i = 0; i < tsx.groups.length; i++) {
                        assertThat("Number of time-series buckets", tsx.ts.getBuckets().size(), equalTo(g));
                        StringTerms terms = tsx.ts.getBuckets().get(i).getAggregations().get("groups");
                        assertThat("Number of terms buckets", terms.getBuckets().size(), equalTo(1));
                        InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("track");
                        assertGeoLine(sortOrder, tsx.groups[i], geoLine, tsx, false);
                    }
                });
            }
        }
    }

    private void assertGeoLine(SortOrder sortOrder, String group, InternalGeoLine geoLine, TestTSAssertionResults tsx, boolean complete) {
        long[] expectedAggPoints = tsx.expectedAggPoints.get(group);
        double[] expectedAggSortValues = tsx.expectedAggSortValues.get(group);
        String prefix = "GeoLine[sort=" + sortOrder + ", use-timestamps=" + tsx.useTimeSeriesAggregation + ", group='" + group + "']";
        assertThat(prefix + " is complete", geoLine.isComplete(), is(complete));
        // old geo_line has a bug whereby it can produce lines with truncation happening in the middle instead of the end
        int checkCount = tsx.useTimeSeriesAggregation ? expectedAggPoints.length : min(expectedAggPoints.length / 2, geoLine.line().length);
        assertThat(prefix + " contents", geoLine.line(), isGeoLine(checkCount, expectedAggPoints));
        double[] sortValues = geoLine.sortVals();
        for (int i = 1; i < sortValues.length; i++) {
            Matcher<Double> sortMatcher = switch (sortOrder) {
                case ASC -> Matchers.greaterThan(sortValues[i - 1]);
                case DESC -> Matchers.lessThan(sortValues[i - 1]);
            };
            assertThat(prefix + " expect ordered '" + sortOrder + "' sort values", sortValues[i], sortMatcher);
        }
        if (checkCount == expectedAggSortValues.length) {
            assertArrayEquals(prefix + " sort values", expectedAggSortValues, sortValues, 0d);
        } else {
            for (int i = 0; i < checkCount; i++) {
                assertThat(prefix + " sort value " + i, expectedAggSortValues[i], equalTo(sortValues[i]));
            }
        }
    }

    private static Matcher<long[]> isGeoLine(int checkCount, long[] line) {
        return new TestGeoLineLongArrayMatcher(checkCount, line);
    }

    private static class TestGeoLineLongArrayMatcher extends TypeSafeMatcher<long[]> {
        private final int checkCount;
        private final long[] expectedLine;
        private final ArrayList<String> failures = new ArrayList<>();

        private TestGeoLineLongArrayMatcher(int checkCount, long[] expectedLine) {
            this.checkCount = checkCount;
            this.expectedLine = expectedLine;
        }

        @Override
        public boolean matchesSafely(long[] actualLine) {
            failures.clear();
            if (checkCount == expectedLine.length && actualLine.length != expectedLine.length) {
                failures.add("Expected length " + expectedLine.length + " but got " + actualLine.length);
            }
            for (int i = 0; i < checkCount; i++) {
                Point actual = asPoint(actualLine[i]);
                Point expected = asPoint(expectedLine[i]);
                if (actual.equals(expected) == false) {
                    failures.add("At line position " + i + " expected " + expected + " but got " + actual);
                }
            }
            return failures.isEmpty();
        }

        @Override
        public void describeMismatchSafely(long[] item, Description description) {
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

    /**
     * Wrapper for points and sort fields that is also usable in the GeometrySimplifier library,
     * allowing us to track which points will survive geometry simplification during geo_line aggregations.
     */
    static class TestSimplifiablePoint extends StreamingGeometrySimplifier.PointError {
        private final double sortField;
        private final long encoded;

        TestSimplifiablePoint(int index, double x, double y, double sortField) {
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

    }

    /** Allow test to use own objects for internal use in geometry simplifier, so we can track the sort-fields together with the points */
    static class TestLine implements Geometry {
        final long[] encodedPoints;
        final double[] sortValues;

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

    static class TestGeometrySimplifierMonitor implements StreamingGeometrySimplifier.Monitor {
        int addedCount;
        int removedCount;

        public void reset() {
            addedCount = 0;
            removedCount = 0;
        }

        @Override
        public void pointAdded(String status, List<SimplificationErrorCalculator.PointLike> points) {
            addedCount++;
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
        }

        @Override
        public void startSimplification(String description, int maxPoints) {}

        @Override
        public void endSimplification(String description, List<SimplificationErrorCalculator.PointLike> points) {}
    }

    /** Wrapping the Streaming GeometrySimplifier allowing the test to extract expected points and their sort fields after simplification */
    static class TestGeometrySimplifier extends StreamingGeometrySimplifier<TestLine> {
        TestGeometrySimplifier(int maxPoints, TestGeometrySimplifierMonitor monitor) {
            super("TestGeometrySimplifier", maxPoints, SimplificationErrorCalculator.TRIANGLE_AREA, monitor);
        }

        @Override
        public TestLine produce() {
            return new TestLine(this.length, this.points);
        }
    }

    private record TestConfig(
        int docCount,
        int maxPoints,
        int missingPointFactor,
        int missingTimestampFactor,
        int groupCount,
        SortOrder sortOrder,
        boolean useTimeSeriesAggregation
    ) {
        @SuppressWarnings("SameParameterValue")
        private GeoLineAggregationBuilder lineAggregationBuilder(String name, String valueField, String sortField) {
            MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(valueField).build();
            GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder(name).point(valueConfig)
                .sortOrder(sortOrder)
                .size(maxPoints);
            if (useTimeSeriesAggregation) {
                // In time-series we do not set the sort field
                return lineAggregationBuilder;
            } else {
                // Without a time-series aggregation, we need to specify the sort-field
                MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(sortField).build();
                return lineAggregationBuilder.sort(sortConfig);
            }
        }
    }

    private static class TestData {
        private final TestConfig t;
        private final String[] groups;
        private final HashMap<String, ArrayList<GeoPoint>> allPoints;
        private final HashMap<String, ArrayList<Long>> allTimestamps;
        private final HashMap<String, long[]> expectedAggPoints;
        private final HashMap<String, double[]> expectedAggSortValues;

        private TestData(TestConfig t) {
            this.t = t;
            this.groups = new String[t.groupCount];
            this.allPoints = new HashMap<>();
            this.allTimestamps = new HashMap<>();
            this.expectedAggPoints = new HashMap<>();
            this.expectedAggSortValues = new HashMap<>();
            build();
        }

        private void build() {
            long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
            double startLat = 45.0;
            double startLon = 90;
            for (int g = 0; g < t.groupCount; g++) {
                groups[g] = "group_" + g;
                ArrayList<GeoPoint> points = new ArrayList<>();
                ArrayList<Long> timestamps = new ArrayList<>();
                ArrayList<TestSimplifiablePoint> expectedAggPointsList = new ArrayList<>();
                // TSDB provides docs in DESC time order, so we generate the data that way to simplify assertions
                for (int i = t.docCount - 1; i >= 0; i--) {
                    double lat = startLat + i * 0.1 + randomDoubleBetween(-0.1, 0.1, false);
                    double lon = startLon - g * 10 + i * 0.1 + randomDoubleBetween(-0.1, 0.1, false);
                    GeoPoint point = (t.missingPointFactor > 0 && i % t.missingPointFactor == 0) ? null : new GeoPoint(lat, lon);
                    Long timestamp = (t.missingTimestampFactor > 0 && i % t.missingTimestampFactor == 0) ? null : startTime + 1000L * i;
                    points.add(point);
                    timestamps.add(timestamp);
                    if (point != null && timestamp != null) {
                        expectedAggPointsList.add(new TestSimplifiablePoint(expectedAggPointsList.size(), lon, lat, (double) timestamp));
                    }
                }
                if (t.missingPointFactor > 0) {
                    int deletedAtLeast = points.size() / t.missingPointFactor;
                    assertThat(
                        "Valid points less than total/" + t.missingPointFactor,
                        expectedAggPointsList.size(),
                        lessThanOrEqualTo(points.size() - deletedAtLeast)
                    );
                }
                if (t.missingTimestampFactor > 0) {
                    int deletedAtLeast = points.size() / t.missingTimestampFactor;
                    assertThat(
                        "Valid points less than total/" + t.missingTimestampFactor,
                        expectedAggPointsList.size(),
                        lessThanOrEqualTo(points.size() - deletedAtLeast)
                    );
                }
                if (t.useTimeSeriesAggregation && t.maxPoints < t.docCount) {
                    // The aggregation will simplify the line in reverse order, so we need to anticipate the same simplification in the
                    // tests
                    TestGeometrySimplifierMonitor monitor = new TestGeometrySimplifierMonitor();
                    var simplifier = new TestGeometrySimplifier(t.maxPoints, monitor);
                    for (TestSimplifiablePoint p : expectedAggPointsList) {
                        simplifier.consume(p);
                    }
                    TestLine line = simplifier.produce();
                    assertThat("Simplifier added points", monitor.addedCount, equalTo(expectedAggPointsList.size()));
                    assertThat("Simplifier Removed points", monitor.removedCount, equalTo(expectedAggPointsList.size() - t.maxPoints));
                    expectedAggPoints.put(groups[g], line.encodedPoints);
                    expectedAggSortValues.put(groups[g], line.sortValues);
                } else {
                    // The aggregation will NOT simplify the line, so we should only anticipate the removal of invalid documents
                    int pointCount = min(t.maxPoints, expectedAggPointsList.size());  // possible truncation if !useTimestampField
                    int offset = t.sortOrder == SortOrder.DESC ? 0 : Math.max(0, expectedAggPointsList.size() - pointCount);
                    long[] xp = new long[pointCount];
                    double[] xv = new double[pointCount];
                    for (int i = 0; i < xp.length; i++) {
                        xp[i] = expectedAggPointsList.get(i + offset).encoded;
                        xv[i] = expectedAggPointsList.get(i + offset).sortField;
                    }
                    expectedAggPoints.put(groups[g], xp);
                    expectedAggSortValues.put(groups[g], xv);
                }
                // TSDB orders in time-descending
                if (t.sortOrder == SortOrder.ASC) {
                    ArrayUtils.reverseSubArray(expectedAggPoints.get(groups[g]), 0, expectedAggPoints.get(groups[g]).length);
                    ArrayUtils.reverseSubArray(expectedAggSortValues.get(groups[g]), 0, expectedAggSortValues.get(groups[g]).length);
                }
                allPoints.put(groups[g], points);
                allTimestamps.put(groups[g], timestamps);
            }
        }

        private ArrayList<GeoPoint> pointsForGroup(int g) {
            return allPoints.get(groups[g]);
        }

        private ArrayList<Long> timestampsForGroup(int g) {
            return allTimestamps.get(groups[g]);
        }

    }

    private record TestTSAssertionResults(
        MultiBucketsAggregation ts,
        boolean useTimeSeriesAggregation,
        String[] groups,
        Map<String, long[]> expectedAggPoints,
        Map<String, double[]> expectedAggSortValues
    ) {
        private TestTSAssertionResults(MultiBucketsAggregation ts, TestConfig testConfig, TestData testData) {
            this(ts, testConfig.useTimeSeriesAggregation, testData.groups, testData.expectedAggPoints, testData.expectedAggSortValues);
        }
    }

    private void assertGeoLine_TSDB(
        TestConfig testConfig,
        Function<GeoLineAggregationBuilder, AggregationBuilder> makeTSAggBuilder,
        Consumer<TestTSAssertionResults> verifyTSResults
    ) throws IOException {
        // Prepare test data
        TestData testData = new TestData(testConfig);

        // Create the nested aggregations to run
        GeoLineAggregationBuilder lineAggregationBuilder = testConfig.lineAggregationBuilder("track", "value_field", "time_field");
        AggregationBuilder aggregationBuilder = makeTSAggBuilder.apply(lineAggregationBuilder);

        // Run the aggregation
        testCase(aggregationBuilder, iw -> {
            for (int g = 0; g < testConfig.groupCount; g++) {
                ArrayList<GeoPoint> points = testData.pointsForGroup(g);
                ArrayList<Long> timestamps = testData.timestampsForGroup(g);
                for (int i = 0; i < points.size(); i++) {
                    var routingFields = new RoutingPathFields(null);
                    routingFields.addString("group_id", testData.groups[g]);
                    ArrayList<Field> fields = new ArrayList<>(
                        Arrays.asList(
                            new SortedDocValuesField("group_id", new BytesRef(testData.groups[g])),
                            new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, routingFields.buildHash().toBytesRef())
                        )
                    );
                    GeoPoint point = points.get(i);
                    if (point != null) {
                        fields.add(new LatLonDocValuesField("value_field", point.lat(), point.lon()));
                    }
                    Long timestamp = timestamps.get(i);
                    if (timestamp != null) {
                        // We add the 'time_field' for the non-time-series tests, and the '@timestamp' field for the time-series tests
                        fields.add(new SortedNumericDocValuesField("time_field", timestamp));
                        // TODO: Do we need both? We found both being used in TimeSeriesAggregatorTests, but why?
                        fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
                        fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
                    }
                    iw.addDocument(fields);
                }
            }
        }, ts -> verifyTSResults.accept(new TestTSAssertionResults(ts, testConfig, testData)));
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
            int lineSize = min(numPoints, size);
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

            lines.put(
                String.valueOf(groupOrd),
                new InternalGeoLine("track", linePoints, lineSorts, null, complete, true, sortOrder, size, false, false)
            );

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
                var metricType = randomBoolean() ? POSITION : null; // metric type does not affect geo_line behaviour
                fieldTypes.add(new GeoPointFieldMapper.GeoPointFieldType("value_field", metricType, TIME_SERIES));
            } else {
                fieldTypes.add(new GeoPointFieldMapper.GeoPointFieldType("value_field"));
            }
            fieldTypes.add(new DateFieldMapper.DateFieldType("time_field"));
            fieldTypes.add(
                new KeywordFieldMapper.Builder("group_id", IndexVersion.current()).dimension(true)
                    .docValues(true)
                    .indexed(false)
                    .build(MapperBuilderContext.root(true, true))
                    .fieldType()
            );
            fieldTypes.add(new NumberFieldMapper.NumberFieldType("sort_field", NumberFieldMapper.NumberType.LONG));
            AggTestConfig aggTestConfig = new AggTestConfig(aggregationBuilder, fieldTypes.toArray(new MappedFieldType[0]));

            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                A terms = (A) searchAndReduce(indexReader, aggTestConfig);
                verify.accept(terms);
            }
        }
    }
}
