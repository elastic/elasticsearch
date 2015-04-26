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
package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid.Bucket;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class GeoHashGridTests extends ElasticsearchIntegrationTest {

    static ObjectIntMap<String> expectedDocCountsForGeoHash = null;
    static ObjectIntMap<String> multiValuedExpectedDocCountsForGeoHash = null;
    static int highestPrecisionGeohash = 12;
    static int numDocs = 100;

    static String smallestGeoHash = null;

    private static IndexRequestBuilder indexCity(String index, String name, List<String> latLon) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().field("city", name);
        if (latLon != null) {
            source = source.field("location", latLon);
        }
        source = source.endObject();
        return client().prepareIndex(index, "type").setSource(source);
    }

    private static IndexRequestBuilder indexCity(String index, String name, String latLon) throws Exception {
        return indexCity(index, name, Arrays.<String>asList(latLon));
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx_unmapped");

        assertAcked(prepareCreate("idx")
                .addMapping("type", "location", "type=geo_point", "city", "type=string,index=not_analyzed"));

        List<IndexRequestBuilder> cities = new ArrayList<>();
        Random random = getRandom();
        expectedDocCountsForGeoHash = new ObjectIntOpenHashMap<>(numDocs * 2);
        for (int i = 0; i < numDocs; i++) {
            //generate random point
            double lat = (180d * random.nextDouble()) - 90d;
            double lng = (360d * random.nextDouble()) - 180d;
            String randomGeoHash = GeoHashUtils.encode(lat, lng, highestPrecisionGeohash);
            //Index at the highest resolution
            cities.add(indexCity("idx", randomGeoHash, lat + ", " + lng));
            expectedDocCountsForGeoHash.put(randomGeoHash, expectedDocCountsForGeoHash.getOrDefault(randomGeoHash, 0) + 1);
            //Update expected doc counts for all resolutions..
            for (int precision = highestPrecisionGeohash - 1; precision > 0; precision--) {
                String hash = GeoHashUtils.encode(lat, lng, precision);
                if ((smallestGeoHash == null) || (hash.length() < smallestGeoHash.length())) {
                    smallestGeoHash = hash;
                }
                expectedDocCountsForGeoHash.put(hash, expectedDocCountsForGeoHash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);

        assertAcked(prepareCreate("multi_valued_idx")
                .addMapping("type", "location", "type=geo_point", "city", "type=string,index=not_analyzed"));

        cities = new ArrayList<>();
        multiValuedExpectedDocCountsForGeoHash = new ObjectIntOpenHashMap<>(numDocs * 2);
        for (int i = 0; i < numDocs; i++) {
            final int numPoints = random.nextInt(4);
            List<String> points = new ArrayList<>();
            Set<String> geoHashes = new HashSet<>();
            for (int j = 0; j < numPoints; ++j) {
                double lat = (180d * random.nextDouble()) - 90d;
                double lng = (360d * random.nextDouble()) - 180d;
                points.add(lat + "," + lng);
                // Update expected doc counts for all resolutions..
                for (int precision = highestPrecisionGeohash; precision > 0; precision--) {
                    final String geoHash = GeoHashUtils.encode(lat, lng, precision);
                    geoHashes.add(geoHash);
                }
            }
            cities.add(indexCity("multi_valued_idx", Integer.toString(i), points));
            for (String hash : geoHashes) {
                multiValuedExpectedDocCountsForGeoHash.put(hash, multiValuedExpectedDocCountsForGeoHash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);

        ensureSearchable();
    }


    @Test
    public void simple() throws Exception {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            List<Bucket> buckets = geoGrid.getBuckets();
            Object[] propertiesKeys = (Object[]) geoGrid.getProperty("_key");
            Object[] propertiesDocCounts = (Object[]) geoGrid.getProperty("_count");
            for (int i = 0; i < buckets.size(); i++) {
                GeoHashGrid.Bucket cell = buckets.get(i);
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeoHash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ",
                        expectedBucketCount, bucketCount);
                GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(GeoHashUtils.encode(geoPoint.lat(), geoPoint.lon(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    @Test
    public void multivalued() throws Exception {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("multi_valued_idx")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            for (GeoHashGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = multiValuedExpectedDocCountsForGeoHash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ",
                        expectedBucketCount, bucketCount);
            }
        }
    }

    @Test
    public void filtered() throws Exception {
        GeoBoundingBoxFilterBuilder bbox = new GeoBoundingBoxFilterBuilder("location");
        bbox.topLeft(smallestGeoHash).bottomRight(smallestGeoHash).filterName("bbox");
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                    .addAggregation(
                            AggregationBuilders.filter("filtered").filter(bbox)
                                    .subAggregation(
                                            geohashGrid("geohashgrid")
                                                    .field("location")
                                                    .precision(precision)
                                    )
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            Filter filter = response.getAggregations().get("filtered");

            GeoHashGrid geoGrid = filter.getAggregations().get("geohashgrid");
            for (GeoHashGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeoHash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertTrue("Buckets must be filtered", geohash.startsWith(smallestGeoHash));
                assertEquals("Geohash " + geohash + " has wrong doc count ",
                        expectedBucketCount, bucketCount);

            }
        }
    }

    @Test
    public void unmapped() throws Exception {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("idx_unmapped")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            assertThat(geoGrid.getBuckets().size(), equalTo(0));
        }

    }

    @Test
    public void partiallyUnmapped() throws Exception {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            for (GeoHashGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeoHash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ",
                        expectedBucketCount, bucketCount);
            }
        }
    }

    @Test
    public void testTopMatch() throws Exception {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .size(1)
                            .shardSize(100)
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);

            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            //Check we only have one bucket with the best match for that resolution
            assertThat(geoGrid.getBuckets().size(), equalTo(1));
            for (GeoHashGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = 0;
                for (ObjectIntCursor<String> cursor : expectedDocCountsForGeoHash) {
                    if (cursor.key.length() == precision) {
                        expectedBucketCount = Math.max(expectedBucketCount, cursor.value);
                    }
                }
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ",
                        expectedBucketCount, bucketCount);
            }
        }
    }

    @Test
    // making sure this doesn't runs into an OOME
    public void sizeIsZero() {
        for (int precision = 1; precision <= highestPrecisionGeohash; precision++) {
            final int size = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
            final int shardSize = randomBoolean() ? -1 : 0;
            SearchResponse response = client().prepareSearch("idx")
                    .addAggregation(geohashGrid("geohashgrid")
                            .field("location")
                            .size(size)
                            .shardSize(shardSize)
                            .precision(precision)
                    )
                    .execute().actionGet();

            assertSearchResponse(response);
            GeoHashGrid geoGrid = response.getAggregations().get("geohashgrid");
            assertThat(geoGrid.getBuckets().size(), greaterThanOrEqualTo(1));
        }
    }

}
