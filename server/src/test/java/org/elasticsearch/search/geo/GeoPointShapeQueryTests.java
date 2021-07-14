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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;

public class GeoPointShapeQueryTests extends GeoPointShapeQueryTestCase {

    @Override
    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(fieldName)
            .field("type", "geo_point")
            .endObject().endObject().endObject();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get();
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultGeoFieldName)
            .field("type", "geo_point")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", defaultGeoFieldName)
            .endObject()
            .endObject()
            .endObject());

        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        Point point = GeometryTestUtils.randomPoint(false);
        client().prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(point)).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .setQuery(geoShapeQuery("alias", point))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }
}
