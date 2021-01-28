/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.transform.transforms.common.AggregationValueExtractor.getExtractor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationValueExtractorTests extends ESTestCase {

    public void testSingleValueAggExtractor() {
        Aggregation agg = createSingleMetricAgg("metric", Double.NaN, "NaN");
        assertThat(getExtractor(agg).value(agg, Collections.singletonMap("metric", "double"), ""), is(nullValue()));

        agg = createSingleMetricAgg("metric", Double.POSITIVE_INFINITY, "NaN");
        assertThat(getExtractor(agg).value(agg, Collections.singletonMap("metric", "double"), ""), is(nullValue()));

        agg = createSingleMetricAgg("metric", 100.0, "100.0");
        assertThat(getExtractor(agg).value(agg, Collections.singletonMap("metric", "double"), ""), equalTo(100.0));

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(getExtractor(agg).value(agg, Collections.singletonMap("metric", "double"), ""), equalTo(100.0));

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(
            getExtractor(agg).value(agg, Collections.singletonMap("metric", "string"), ""),
            equalTo("one_hundred")
        );

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(
            getExtractor(agg).value(agg, Collections.singletonMap("metric", "unsigned_long"), ""),
            equalTo(100L)
        );
    }

    public static NumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, Double value, String valueAsString) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.value()).thenReturn(value);
        when(agg.getValueAsString()).thenReturn(valueAsString);
        when(agg.getName()).thenReturn(name);
        return agg;
    }


    private ScriptedMetric createScriptedMetric(Object returnValue) {
        ScriptedMetric agg = mock(ScriptedMetric.class);
        when(agg.aggregation()).thenReturn(returnValue);
        return agg;
    }

    @SuppressWarnings("unchecked")
    public void testScriptedMetricAggExtractor() {
        Aggregation agg = createScriptedMetric(null);
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), is(nullValue()));

        agg = createScriptedMetric(Collections.singletonList("values"));
        Object val = getExtractor(agg).value(agg, Collections.emptyMap(), "");
        assertThat((List<String>) val, hasItem("values"));

        agg = createScriptedMetric(Collections.singletonMap("key", 100));
        val = getExtractor(agg).value(agg, Collections.emptyMap(), "");
        assertThat(((Map<String, Object>) val).get("key"), equalTo(100));
    }

    private GeoCentroid createGeoCentroid(GeoPoint point, long count) {
        GeoCentroid agg = mock(GeoCentroid.class);
        when(agg.centroid()).thenReturn(point);
        when(agg.count()).thenReturn(count);
        return agg;
    }

    public void testGeoCentroidAggExtractor() {
        Aggregation agg = createGeoCentroid(null, 0);
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), is(nullValue()));

        agg = createGeoCentroid(new GeoPoint(100.0, 101.0), 0);
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), is(nullValue()));

        agg = createGeoCentroid(new GeoPoint(100.0, 101.0), randomIntBetween(1, 100));
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), equalTo("100.0, 101.0"));
    }

    private GeoBounds createGeoBounds(GeoPoint tl, GeoPoint br) {
        GeoBounds agg = mock(GeoBounds.class);
        when(agg.bottomRight()).thenReturn(br);
        when(agg.topLeft()).thenReturn(tl);
        return agg;
    }

    @SuppressWarnings("unchecked")
    public void testGeoBoundsAggExtractor() {
        final int numberOfRuns = 25;
        Aggregation agg = createGeoBounds(null, new GeoPoint(100.0, 101.0));
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), is(nullValue()));

        agg = createGeoBounds(new GeoPoint(100.0, 101.0), null);
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), is(nullValue()));

        String type = "point";
        for (int i = 0; i < numberOfRuns; i++) {
            Map<String, Object> expectedObject = new HashMap<>();
            expectedObject.put("type", type);
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            expectedObject.put("coordinates", Arrays.asList(lon, lat));
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat, lon));
            assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), equalTo(expectedObject));
        }

        type = "linestring";
        for (int i = 0; i < numberOfRuns; i++) {
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            double lat2 = lat;
            double lon2 = lon;
            if (randomBoolean()) {
                lat2 = randomDoubleBetween(-90.0, 90.0, false);
            } else {
                lon2 = randomDoubleBetween(-180.0, 180.0, false);
            }
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat2, lon2));
            Object val = getExtractor(agg).value(agg, Collections.emptyMap(), "");
            Map<String, Object> geoJson = (Map<String, Object>) val;
            assertThat(geoJson.get("type"), equalTo(type));
            List<Double[]> coordinates = (List<Double[]>) geoJson.get("coordinates");
            for (Double[] coor : coordinates) {
                assertThat(coor.length, equalTo(2));
            }
            assertThat(coordinates.get(0)[0], equalTo(lon));
            assertThat(coordinates.get(0)[1], equalTo(lat));
            assertThat(coordinates.get(1)[0], equalTo(lon2));
            assertThat(coordinates.get(1)[1], equalTo(lat2));
        }

        type = "polygon";
        for (int i = 0; i < numberOfRuns; i++) {
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            double lat2 = randomDoubleBetween(-90.0, 90.0, false);
            double lon2 = randomDoubleBetween(-180.0, 180.0, false);
            while (Double.compare(lat, lat2) == 0 || Double.compare(lon, lon2) == 0) {
                lat2 = randomDoubleBetween(-90.0, 90.0, false);
                lon2 = randomDoubleBetween(-180.0, 180.0, false);
            }
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat2, lon2));
            Object val = getExtractor(agg).value(agg, Collections.emptyMap(), "");
            Map<String, Object> geoJson = (Map<String, Object>) val;
            assertThat(geoJson.get("type"), equalTo(type));
            List<List<Double[]>> coordinates = (List<List<Double[]>>) geoJson.get("coordinates");
            assertThat(coordinates.size(), equalTo(1));
            assertThat(coordinates.get(0).size(), equalTo(5));
            List<List<Double>> expected = Arrays.asList(
                Arrays.asList(lon, lat),
                Arrays.asList(lon2, lat),
                Arrays.asList(lon2, lat2),
                Arrays.asList(lon, lat2),
                Arrays.asList(lon, lat)
            );
            for (int j = 0; j < 5; j++) {
                Double[] coordinate = coordinates.get(0).get(j);
                assertThat(coordinate.length, equalTo(2));
                assertThat(coordinate[0], equalTo(expected.get(j).get(0)));
                assertThat(coordinate[1], equalTo(expected.get(j).get(1)));
            }
        }
    }

    public static Percentiles createPercentilesAgg(String name, List<Percentile> percentiles) {
        Percentiles agg = mock(Percentiles.class);

        when(agg.iterator()).thenReturn(percentiles.iterator());
        when(agg.getName()).thenReturn(name);
        return agg;
    }

    public void testPercentilesAggExtractor() {
        Aggregation agg = createPercentilesAgg(
            "p_agg",
            Arrays.asList(new Percentile(1, 0), new Percentile(50, 22.2), new Percentile(99, 43.3), new Percentile(99.5, 100.3))
        );
        assertThat(
            getExtractor(agg).value(agg, Collections.emptyMap(), ""),
            equalTo(asMap("1", 0.0, "50", 22.2, "99", 43.3, "99_5", 100.3))
        );
    }

    public void testPercentilesAggExtractorNaN() {
        Aggregation agg = createPercentilesAgg("p_agg", Arrays.asList(new Percentile(1, Double.NaN), new Percentile(50, Double.NaN)));
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), equalTo(asMap("1", null, "50", null)));
    }

    public static SingleBucketAggregation createSingleBucketAgg(String name, long docCount, Aggregation... subAggregations) {
        SingleBucketAggregation agg = mock(SingleBucketAggregation.class);
        when(agg.getDocCount()).thenReturn(docCount);
        when(agg.getName()).thenReturn(name);
        if (subAggregations != null) {
            org.elasticsearch.search.aggregations.Aggregations subAggs = new org.elasticsearch.search.aggregations.Aggregations(
                Arrays.asList(subAggregations)
            );
            when(agg.getAggregations()).thenReturn(subAggs);
        } else {
            when(agg.getAggregations()).thenReturn(null);
        }
        return agg;
    }

    public void testSingleBucketAggExtractor() {
        Aggregation agg = createSingleBucketAgg("sba", 42L);
        assertThat(getExtractor(agg).value(agg, Collections.emptyMap(), ""), equalTo(42L));

        agg = createSingleBucketAgg("sba1", 42L, createSingleMetricAgg("sub1", 100.0, "100.0"));
        assertThat(
            getExtractor(agg).value(agg, Collections.emptyMap(), ""),
            equalTo(Collections.singletonMap("sub1", 100.0))
        );

        agg = createSingleBucketAgg(
            "sba2",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three")
        );
        assertThat(
            getExtractor(agg).value(agg, asStringMap("sba2.sub1", "long", "sba2.sub2", "float"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33))
        );

        agg = createSingleBucketAgg(
            "sba3",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three"),
            createSingleBucketAgg("sub3", 42L)
        );
        assertThat(
            getExtractor(agg).value(agg, asStringMap("sba3.sub1", "long", "sba3.sub2", "double"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33, "sub3", 42L))
        );

        agg = createSingleBucketAgg(
            "sba4",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three"),
            createSingleBucketAgg("sub3", 42L, createSingleMetricAgg("subsub1", 11.1, "eleven_dot_eleven"))
        );
        assertThat(
            getExtractor(agg)
                .value(agg, asStringMap("sba4.sub3.subsub1", "double", "sba4.sub2", "float", "sba4.sub1", "long"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33, "sub3", asMap("subsub1", 11.1)))
        );
    }

    public void testDefaultBucketKeyExtractor() {
        AggregationValueExtractor.BucketKeyExtractor extractor = new AggregationValueExtractor.DefaultBucketKeyExtractor();

        assertThat(extractor.value(42.0, "long"), equalTo(42L));
        assertThat(extractor.value(42.2, "double"), equalTo(42.2));
        assertThat(extractor.value(1577836800000L, "date"), equalTo("2020-01-01T00:00:00.000Z"));
        assertThat(extractor.value(1577836800000L, "date_nanos"), equalTo("2020-01-01T00:00:00.000Z"));
        assertThat(extractor.value(1577836800000L, "long"), equalTo(1577836800000L));
    }

    public void testDatesAsEpochBucketKeyExtractor() {
        AggregationValueExtractor.BucketKeyExtractor extractor = new AggregationValueExtractor.DatesAsEpochBucketKeyExtractor();

        assertThat(extractor.value(42.0, "long"), equalTo(42L));
        assertThat(extractor.value(42.2, "double"), equalTo(42.2));
        assertThat(extractor.value(1577836800000L, "date"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, "date_nanos"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, "long"), equalTo(1577836800000L));
    }

    static Map<String, Object> asMap(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            map.put(field, fields[i + 1]);
        }
        return map;
    }

    static Map<String, String> asStringMap(String... strings) {
        assert strings.length % 2 == 0;
        final Map<String, String> map = new HashMap<>();
        for (int i = 0; i < strings.length; i += 2) {
            String field = strings[i];
            map.put(field, strings[i + 1]);
        }
        return map;
    }
}
