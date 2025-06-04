/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.util.List;

import static org.elasticsearch.search.aggregations.metrics.InternalGeoBoundsTests.GEOHASH_TOLERANCE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class GeoBoundsAggregatorTests extends AggregatorTestCase {
    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("field").wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                InternalGeoBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testUnmappedFieldWithDocs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            if (randomBoolean()) {
                Document doc = new Document();
                doc.add(new LatLonDocValuesField("field", 0.0, 0.0));
                w.addDocument(doc);
            }

            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("non_existent").wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                InternalGeoBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");

            Point point = GeometryTestUtils.randomPoint(false);
            double lon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getX()));
            double lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getY()));

            // valid missing values
            for (Object missingVal : List.of("POINT(" + lon + " " + lat + ")", lat + ", " + lon, new GeoPoint(lat, lon))) {
                GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("field")
                    .missing(missingVal)
                    .wrapLongitude(false);

                try (IndexReader reader = w.getReader()) {
                    InternalGeoBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                    assertThat(bounds.top, equalTo(lat));
                    assertThat(bounds.bottom, equalTo(lat));
                    assertThat(bounds.posLeft, equalTo(lon >= 0 ? lon : Double.POSITIVE_INFINITY));
                    assertThat(bounds.posRight, equalTo(lon >= 0 ? lon : Double.NEGATIVE_INFINITY));
                    assertThat(bounds.negLeft, equalTo(lon >= 0 ? Double.POSITIVE_INFINITY : lon));
                    assertThat(bounds.negRight, equalTo(lon >= 0 ? Double.NEGATIVE_INFINITY : lon));
                }
            }
        }
    }

    public void testInvalidMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");

            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("field")
                .missing("invalid")
                .wrapLongitude(false);
            try (IndexReader reader = w.getReader()) {
                ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class, () -> {
                    searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                });
                assertThat(exception.getMessage(), startsWith("unsupported symbol"));
            }
        }
    }

    public void testRandom() throws Exception {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                for (int j = 0; j < numValues; j++) {
                    GeoPoint point = RandomGeoGenerator.randomPoint(random());
                    if (point.getLat() > top) {
                        top = point.getLat();
                    }
                    if (point.getLat() < bottom) {
                        bottom = point.getLat();
                    }
                    if (point.getLon() >= 0 && point.getLon() < posLeft) {
                        posLeft = point.getLon();
                    }
                    if (point.getLon() >= 0 && point.getLon() > posRight) {
                        posRight = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() < negLeft) {
                        negLeft = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() > negRight) {
                        negRight = point.getLon();
                    }
                    doc.add(new LatLonDocValuesField("field", point.getLat(), point.getLon()));
                }
                w.addDocument(doc);
            }
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("field").wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                InternalGeoBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertThat(bounds.top, closeTo(top, GEOHASH_TOLERANCE));
                assertThat(bounds.bottom, closeTo(bottom, GEOHASH_TOLERANCE));
                assertThat(bounds.posLeft, closeTo(posLeft, GEOHASH_TOLERANCE));
                assertThat(bounds.posRight, closeTo(posRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negRight, closeTo(negRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negLeft, closeTo(negLeft, GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new GeoBoundsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.GEOPOINT);
    }
}
