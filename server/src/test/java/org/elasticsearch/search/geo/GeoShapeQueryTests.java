/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class GeoShapeQueryTests extends GeoShapeQueryTestCase {

    @Override
    protected void createMapping(String indexName, String type, String fieldName, Settings settings) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping(type, xcb).get();
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultGeoFieldName)
                .field("type", "geo_shape")
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultGeoFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate(defaultIndexName).addMapping(defaultType, mapping, XContentType.JSON).get();
        ensureGreen();

        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex(defaultIndexName, defaultType)
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQuery("alias", multiPoint)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }
}
