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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class GeoLineAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new SpatialPlugin());
    }

    // test that missing values are ignored
    public void testMixedMissingValues() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("_name");
            assertThat(geoLine.length(), equalTo(2));
            assertTrue(geoLine.isComplete());
            assertArrayEquals(expectedAggPoints, geoLine.line());
            assertArrayEquals(expectedAggSortValues, geoLine.sortVals(), 0d);
        });
    }

    public void testMissingGeoPointValueField() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("_name");
            assertThat(geoLine.length(), equalTo(0));
            assertTrue(geoLine.isComplete());
        });
    }

    public void testMissingSortField() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(10);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("_name");
            assertThat(geoLine.length(), equalTo(0));
            assertTrue(geoLine.isComplete());
        });
    }

    public void testMissingSortField_TSDB() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig).size(10);

        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
            .subAggregation(lineAggregationBuilder);

        TimeSeriesAggregationBuilder aggregationBuilder = new TimeSeriesAggregationBuilder("tsid").subAggregation(termsAggregationBuilder);

        double startLat = 45.0;
        double startLon = 90;
        long lonLat = (((long) GeoEncodingUtils.encodeLongitude(startLon)) << 32) | GeoEncodingUtils.encodeLatitude(startLat) & 0xffffffffL;
        // input
        long[] points = new long[] { lonLat, 0, lonLat, 0, lonLat, lonLat, lonLat };
        // expected
        long[] expectedAggPoints = new long[] { lonLat, lonLat, lonLat, lonLat, lonLat };
        double[] expectedAggSortValues = new double[] {
            NumericUtils.doubleToSortableLong(1),
            NumericUtils.doubleToSortableLong(2),
            NumericUtils.doubleToSortableLong(3),
            NumericUtils.doubleToSortableLong(4),
            NumericUtils.doubleToSortableLong(5) };

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        testCase(aggregationBuilder, iw -> {
            for (int i = 0; i < points.length; i++) {
                long timestamp = startTime + 1000 * i;
                double latitude = startLat + i * 0.1;
                double longitude = startLon + i * 0.1;
                iw.addDocument(
                    Arrays.asList(
                        new LatLonDocValuesField("value_field", latitude, longitude),
                        new SortedDocValuesField("group_id", new BytesRef("group")),
                        new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp),
                        new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp)
                    )
                );
            }
        }, terms -> {
            assertThat(terms.getBuckets().size(), equalTo(1));
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("_name");
            assertThat(geoLine.length(), equalTo(expectedAggPoints.length));
            assertTrue(geoLine.isComplete());
            assertArrayEquals(expectedAggPoints, geoLine.line());
            assertArrayEquals(expectedAggSortValues, geoLine.sortVals(), 0d);
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
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
        new PathArraySorter(linePoints, lineSorts, SortOrder.ASC).sort();

        lines.put(groupOrd, new InternalGeoLine("_name", linePoints, lineSorts, null, complete, true, SortOrder.ASC, size));

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
            for (Terms.Bucket bucket : terms.getBuckets()) {
                InternalGeoLine expectedGeoLine = lines.get(bucket.getKeyAsString());
                InternalGeoLine geoLine = bucket.getAggregations().get("_name");
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
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
            .subAggregation(lineAggregationBuilder);
        testCase(aggregationBuilder, iw -> {}, terms -> { assertTrue(terms.getBuckets().isEmpty()); });
    }

    public void testOnePoint() throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(SortOrder.ASC)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
            InternalGeoLine geoLine = terms.getBuckets().get(0).getAggregations().get("_name");
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

    private void testAggregator(SortOrder sortOrder) throws IOException {
        int size = randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(valueConfig)
            .sortOrder(sortOrder)
            .sort(sortConfig)
            .size(size);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("group_id")
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
            new PathArraySorter(linePoints, lineSorts, sortOrder).sort();

            lines.put(String.valueOf(groupOrd), new InternalGeoLine("_name", linePoints, lineSorts, null, complete, true, sortOrder, size));

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
            for (Terms.Bucket bucket : terms.getBuckets()) {
                InternalGeoLine expectedGeoLine = lines.get(bucket.getKeyAsString());
                InternalGeoLine geoLine = bucket.getAggregations().get("_name");
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

    private void testCase(
        AggregationBuilder aggregationBuilder,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<Terms> verify
    ) throws IOException {

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();
        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        try {
            boolean timeSeries = aggregationBuilder.isInSortOrderExecutionRequired();
            TimeSeriesParams.MetricType metricType = timeSeries ? TimeSeriesParams.MetricType.POSITION : null;
            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("value_field", metricType);
            MappedFieldType groupFieldType = new KeywordFieldMapper.KeywordFieldType("group_id", false, true, Collections.emptyMap());
            MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("sort_field", NumberFieldMapper.NumberType.LONG);

            Terms terms = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType, fieldType2, groupFieldType));
            verify.accept(terms);
        } finally {
            indexReader.close();
            directory.close();
        }
    }
}
