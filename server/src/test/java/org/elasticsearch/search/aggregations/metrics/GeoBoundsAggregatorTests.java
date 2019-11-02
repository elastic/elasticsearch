/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.geo.GeoExtent;
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
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(Double.isInfinite(bounds.extent.maxLat()));
                assertTrue(Double.isInfinite(bounds.extent.minLat()));
                assertTrue(Double.isInfinite(bounds.extent.minLat()));
                assertTrue(Double.isInfinite(bounds.extent.maxLat()));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testRandom() throws Exception {
        GeoExtent extent  = new GeoExtent();
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                for (int j = 0; j < numValues; j++) {
                    GeoPoint point = RandomGeoGenerator.randomPoint(random());
                    extent.addPoint(point.lat(), point.lon());
                    doc.add(new LatLonDocValuesField("field", point.getLat(), point.getLon()));
                }
                w.addDocument(doc);
            }
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertThat(bounds.extent.maxLat(), closeTo(extent.maxLat(), GEOHASH_TOLERANCE));
                assertThat(bounds.extent.minLat(), closeTo(extent.minLat(), GEOHASH_TOLERANCE));
                assertThat(bounds.extent.minLon(true), closeTo(extent.minLon(true), GEOHASH_TOLERANCE));
                assertThat(bounds.extent.minLon(false), closeTo(extent.minLon(false), GEOHASH_TOLERANCE));
                assertThat(bounds.extent.maxLon(true), closeTo(extent.maxLon(true), GEOHASH_TOLERANCE));
                assertThat(bounds.extent.maxLon(false), closeTo(extent.maxLon(false), GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }
}
