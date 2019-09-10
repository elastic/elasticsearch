/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.locationtech.jts.geom.Coordinate;

import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeProjectedQueryTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, SpatialPlugin.class, XPackPlugin.class);
    }

    private XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .startObject("crs")
            .field("type", "name")
            .startObject("properties")
            .field("name", "EPSG:32614")  // UTM Zone 14N
            .endObject()
            .endObject()
            .endObject().endObject().endObject().endObject();

        return xcb;
    }

    public void testQueryProjectedShape() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        // test index triangle in UTM Zone 14N
        client().prepareIndex("test", "type1", "UTMZ14N").setSource(jsonBuilder().startObject()
            .field("name", "UTM Zone 14N")
            .startObject("location")
            .field("type", "polygon")
            .startArray("coordinates").startArray()
            .startArray().value(166021.4431).value(0.0).endArray()
            .startArray().value(833978.5569).value(9329005.1825).endArray()
            .startArray().value(833978.5569).value(0.0).endArray()
            .startArray().value(166021.4431).value(0.0).endArray() // close the polygon
            .endArray().endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder query = new EnvelopeBuilder(new Coordinate(166000.0, 9350000.0), new Coordinate(833980.0, -10.0));

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("location", query.buildGeometry()))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
    }
}
