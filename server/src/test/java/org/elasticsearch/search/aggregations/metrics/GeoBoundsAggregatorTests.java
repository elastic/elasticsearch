/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import static org.elasticsearch.search.aggregations.metrics.InternalGeoBoundsTests.GEOHASH_TOLERANCE;
import static org.hamcrest.Matchers.closeTo;

public class GeoBoundsAggregatorTests extends AggregatorTestCase {
    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg").field("field").wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
}
