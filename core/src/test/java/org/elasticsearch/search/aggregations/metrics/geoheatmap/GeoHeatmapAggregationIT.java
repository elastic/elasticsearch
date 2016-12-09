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
package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import com.vividsolutions.jts.geom.Coordinate;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmap;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.hamcrest.Matchers;
import org.locationtech.spatial4j.shape.Rectangle;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmapAggregationBuilder.heatmap;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomPoint;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomRectangle;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Indexes a few random docs with geo_shapes and performs basic checks on the
 * heatmap over them
 */
@ESIntegTestCase.SuiteScopeTestCase
public class GeoHeatmapAggregationIT extends ESIntegTestCase {

    static int numDocs, numTag1Docs;
    static Rectangle mbr;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        mbr = xRandomRectangle(random(), xRandomPoint(random()), true);
        createIndex("idx2");
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollectionWithin(random(), mbr, numTag1Docs);
        List<IndexRequestBuilder> builders = new ArrayList<>();

        prepareCreate("idx").addMapping("type", "location", "type=geo_shape,tree=quadtree").execute().actionGet();
        ensureGreen();

        for (int i = 0; i < numTag1Docs; i++) {
            builders.add(client().prepareIndex("idx", "type", "" + i).setSource(jsonBuilder().startObject().field("value", i + 1)
                    .field("tag", "tag1").field("location", gcb.getShapeAt(i)).endObject()));
        }
        for (int i = numTag1Docs; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject().field("value", i).field("tag", "tag2").field("name", "name" + i)
                    .endObject();
            builders.add(client().prepareIndex("idx", "type", "" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted
                // docs that match the filter
                builders.add(client().prepareIndex("idx", "type", "" + i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i)
                    .setSource(jsonBuilder().startObject().field("value", i * 2).endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    /**
     * Create a simple heatmap over the indexed docs and verify that no cell has
     * more than that number of docs
     */
    public void testSimple() throws Exception {

        EnvelopeBuilder env = new EnvelopeBuilder(new Coordinate(mbr.getMinX(), mbr.getMaxY()),
                new Coordinate(mbr.getMaxX(), mbr.getMinY()));
        GeoShapeQueryBuilder geo = QueryBuilders.geoShapeQuery("location", env).relation(ShapeRelation.WITHIN);

        SearchResponse response2 = client().prepareSearch("idx")
                .addAggregation(heatmap("heatmap1").geom(geo).field("location").gridLevel(7).maxCells(100)).execute().actionGet();

        assertSearchResponse(response2);

        GeoHeatmap filter2 = response2.getAggregations().get("heatmap1");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getName(), equalTo("heatmap1"));

        int maxHeatmapValue = 0;
        for (int i = 0; i < filter2.getCounts().length; i++) {
            maxHeatmapValue = Math.max(maxHeatmapValue, filter2.getCounts()[i]);
        }
        assertTrue(maxHeatmapValue <= numTag1Docs);
        
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        response2.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        logger.info("Full heatmap Response Content:\n{ {} }", builder.string());
    }

    /**
     * Test that the number of cells generated is not greater than maxCells
     */
    public void testMaxCells() throws Exception {
        int maxCells = randomIntBetween(1, 50_000) * 2;
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(heatmap("heatmap1").field("location").gridLevel(1).maxCells(maxCells)).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(new Long(numDocs)));
        GeoHeatmap heatmap = searchResponse.getAggregations().get("heatmap1");
        assertThat(heatmap, Matchers.notNullValue());
        assertThat(heatmap.getRows() * heatmap.getColumns(), lessThanOrEqualTo(maxCells));
    }

}
