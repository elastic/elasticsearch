/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

public class GeoShapeQueryTests extends ESSingleNodeTestCase {
    protected static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE
    };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, SpatialPlugin.class, XPackPlugin.class);
    }

    protected XContentBuilder createRandomMapping() throws Exception {
        boolean hasDocValues = true; //randomBoolean();
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("doc_values", hasDocValues)
            .field("type", "geo_shape");
        if (hasDocValues == false && randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
                .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject();

        return xcb;
    }
    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        String mapping = Strings.toString(createRandomMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = jsonBuilder().startObject().startObject("geo")
            .field("type", "geometrycollection")
            .startArray("geometries")
            .startObject()
            .field("type", "point")
            .startArray("coordinates")
            .value(100.0).value(0.0)
            .endArray()
            .endObject()
            .startObject()
            .field("type", "linestring")
            .startArray("coordinates")
            .startArray()
            .value(101.0).value(0.0)
            .endArray()
            .startArray()
            .value(102.0).value(1.0)
            .endArray()
            .endArray()
            .endObject()
            .endArray()
            .endObject().endObject();
        client().prepareIndex("test").setId("1")
            .setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder()
                .polygon(
                    new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0)
                        .coordinate(103.0, 3.0).coordinate(103.0, -1.0)
                        .coordinate(99.0, -1.0)))).relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder().polygon(
                new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                    .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                    .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery("geo", new GeometryCollectionBuilder()
            .polygon(new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0).coordinate(103.0, 3.0)
                .coordinate(103.0, -1.0).coordinate(99.0, -1.0)))
            .polygon(
                new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                    .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                    .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        // no shape
        filter = QueryBuilders.geoShapeQuery("geo", new GeometryCollectionBuilder());
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
    }
}
