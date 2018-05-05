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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.GeoHashGridTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class GeoHashGridAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "location";

    public void testNoDocs() throws IOException {
        final GeoHashType type = GeoHashGridTests.randomType();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, type, GeoHashGridTests.randomPrecision(type), iw -> {
            // Intentionally not writing any docs
        }, geoHashGrid -> assertEquals(0, geoHashGrid.getBuckets().size()));
    }

    public void testFieldMissing() throws IOException {
        final GeoHashType type = GeoHashGridTests.randomType();
        testCase(new MatchAllDocsQuery(), "wrong_field", type, GeoHashGridTests.randomPrecision(type), iw -> {
            iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
        }, geoHashGrid -> assertEquals(0, geoHashGrid.getBuckets().size()));
    }

    public void testHashcodeWithSeveralDocs() throws IOException {
        final GeoHashType type = GeoHashGridTests.randomType();
        testWithSeveralDocs(type, GeoHashGridTests.randomPrecision(type));
    }

    private void testWithSeveralDocs(GeoHashType type, int precision)
            throws IOException {
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, type, precision, iw -> {
            List<LatLonDocValuesField> points = new ArrayList<>();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int pointId = 0; pointId < numPoints; pointId++) {
                double lat = (180d * randomDouble()) - 90d;
                double lng = (360d * randomDouble()) - 180d;

                // Precision-adjust longitude/latitude to avoid wrong bucket placement
                lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                points.add(new LatLonDocValuesField(FIELD_NAME, lat, lng));
                String hash = type.getHandler().hashAsString(type.getHandler().calculateHash(lng, lat, precision));
                if (distinctHashesPerDoc.contains(hash) == false) {
                    expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                }
                distinctHashesPerDoc.add(hash);
                if (usually()) {
                    iw.addDocument(points);
                    points.clear();
                    distinctHashesPerDoc.clear();
                }
            }
            if (points.size() != 0) {
                iw.addDocument(points);
            }
        }, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoHashGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
        });
    }

    private void testCase(Query query, String field, GeoHashType type, int precision,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoHashGrid> verify) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GeoGridAggregationBuilder aggregationBuilder = new GeoGridAggregationBuilder("_name", type).field(field);
        aggregationBuilder.precision(precision);
        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
        fieldType.setHasDocValues(true);
        fieldType.setName(FIELD_NAME);

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalGeoHashGrid) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
