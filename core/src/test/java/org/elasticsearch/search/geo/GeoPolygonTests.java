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

package org.elasticsearch.search.geo;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class GeoPolygonTests extends ElasticsearchSingleNodeTest {

    @Before
    protected void setup() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true)
                .startObject("fielddata").field("format", "doc_values").endObject().endObject().endObject()
                .endObject().endObject();
        createIndex("test", Settings.EMPTY, "type1", xContentBuilder);
        ensureGreen();

        BulkRequestBuilder bulkRequest = client().prepareBulk().setRefresh(true);

        bulkRequest.add(client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "New York")
                        .startObject("location").field("lat", 40.714).field("lon", -74.006).endObject()
                    .endObject()));
        // to NY: 5.286 km
        bulkRequest.add(client().prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Times Square")
                        .startObject("location").field("lat", 40.759).field("lon", -73.984).endObject()
                    .endObject()));
        // to NY: 0.4621 km
        bulkRequest.add(client().prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Tribeca")
                        .startObject("location").field("lat", 40.718).field("lon", -74.008).endObject()
                    .endObject()));
        // to NY: 1.055 km
        bulkRequest.add(client().prepareIndex("test", "type1", "4")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Wall Street")
                        .startObject("location").field("lat", 40.705).field("lon", -74.009).endObject()
                    .endObject()));
        // to NY: 1.258 km
        bulkRequest.add(client().prepareIndex("test", "type1", "5")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Soho")
                        .startObject("location").field("lat", 40.725).field("lon", -74).endObject()
                    .endObject()));
        // to NY: 2.029 km
        bulkRequest.add(client().prepareIndex("test", "type1", "6")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location").field("lat", 40.731).field("lon", -73.996).endObject()
                    .endObject()));
        // to NY: 8.572 km
        bulkRequest.add(client().prepareIndex("test", "type1", "7")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("name", "Brooklyn")
                        .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                    .endObject()));

        bulkRequest.get();
    }

    @Test
    public void simplePolygonTest() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location")
                        .addPoint(40.7, -74.0)
                        .addPoint(40.7, -74.1)
                        .addPoint(40.8, -74.1)
                        .addPoint(40.8, -74.0)
                        .addPoint(40.7, -74.0)))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    @Test
    public void simpleUnclosedPolygon() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location")
                        .addPoint(40.7, -74.0)
                        .addPoint(40.7, -74.1)
                        .addPoint(40.8, -74.1)
                        .addPoint(40.8, -74.0)))
                .execute().actionGet();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    @Test
    public void testBoundingBoxOptimization() throws Exception {
        // Check that the bounding box doesn't affect what the polygon filter returns (and so
        // is truly just an optimization).
        long numHits = -1;
        for (String bboxType : Arrays.asList("none", "memory", "indexed")) {
            SearchResponse searchResponse = client().prepareSearch("test") // from NY
                    .setQuery(geoPolygonQuery("location")
                            .optimizeBbox(bboxType)
                            .addPoint(40.7, -74.0)
                            .addPoint(40.7, -74.1)
                            .addPoint(40.75, -74.2)
                            .addPoint(40.8, -74.1)
                            .addPoint(40.75, -74.05)
                            .addPoint(40.8, -74.0)
                            .addPoint(40.7, -74.0))
                    .execute().actionGet();

            assertSearchResponse(searchResponse);
            logger.info("{} -> {} hits", bboxType, searchResponse.getHits().totalHits());
            if (numHits < 0) {
                numHits = searchResponse.getHits().getTotalHits();
            } else {
                assertThat(searchResponse.getHits().getTotalHits(), equalTo(numHits));
            }
        }
    }

    @Test
    public void testBoundingBoxAppliedFirst() throws Exception {
        final GeoPoint[] points = new GeoPoint[]{
                new GeoPoint(40.7, -74.0),
                new GeoPoint(40.7, -74.1),
                new GeoPoint(40.75, -74.2),
                new GeoPoint(40.8, -74.1),
                new GeoPoint(40.75, -74.05),
                new GeoPoint(40.8, -74.0),
                new GeoPoint(40.7, -74.0)};

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService("test");

        @SuppressWarnings("null")
        GeoPointFieldType fieldType = (GeoPointFieldType) indexService.mapperService().smartNameFieldType("location");
        IndexGeoPointFieldData indexFieldData = indexService.fieldData().getForField(fieldType);

        Map<String, Integer> expectedNumChecks = ImmutableMap.of(
                "none", 7,
                "memory", 4,
                "indexed", 4);

        for (String bboxType : expectedNumChecks.keySet()) {
            CountingGeoPolygonQuery query = new CountingGeoPolygonQuery(
                    points, fieldType, indexFieldData, bboxType);

            for (int shardId : indexService.shardIds()) {
                @SuppressWarnings("null")
                Searcher searcher = indexService.shard(shardId).acquireSearcher("test");
                searcher.searcher().search(query, 1000 /* an arbitrary large number */);
                searcher.close();
            }

            assertThat(query.getNumChecks(), equalTo(expectedNumChecks.get(bboxType)));
        }
    }

    private static class CountingGeoPolygonQuery extends GeoPolygonQuery {
        private final AtomicInteger numChecks;

        public CountingGeoPolygonQuery(GeoPoint[] points, GeoPointFieldType fieldType,
                IndexGeoPointFieldData indexFieldData, String optimizeBbox) {
            super(points, fieldType, indexFieldData, optimizeBbox);
            numChecks = new AtomicInteger(0);
        }

        @Override
        protected boolean containedInPolygon(double lat, double lon) {
            numChecks.incrementAndGet();
            return super.containedInPolygon(lat, lon);
        }

        public int getNumChecks() {
            return numChecks.get();
        }
    }
}

