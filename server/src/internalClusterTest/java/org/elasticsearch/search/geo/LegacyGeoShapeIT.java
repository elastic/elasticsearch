/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGeoShapeIT extends GeoShapeIntegTestCase {

    @Override
    protected void getGeoShapeMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
        b.field("strategy", "recursive");
    }

    @Override
    protected Version getVersion() {
        // legacy shapes can only be created in version lower than 8.x
        return VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
    }

    @Override
    protected boolean allowExpensiveQueries() {
        return false;
    }

    public void testMappingUpdate() {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings(getVersion()).build())
            .setMapping("shape", "type=geo_shape,strategy=recursive").get());
        ensureGreen();

        String update ="{\n" +
            "  \"properties\": {\n" +
            "    \"shape\": {\n" +
            "      \"type\": \"geo_shape\"" +
            "    }\n" +
            "  }\n" +
            "}";

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices()
            .preparePutMapping("test")
            .setSource(update, XContentType.JSON).get());
        assertThat(e.getMessage(), containsString("mapper [shape] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
    }

    /**
     * Test that the circle is still supported for the legacy shapes
     */
    public void testLegacyCircle() throws Exception {
        // create index
        assertAcked(prepareCreate("test").setSettings(settings(getVersion()).build())
            .setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash").get());
        ensureGreen();

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
            builder.startObject().field("type", "circle")
                .startArray("coordinates").value(30).value(50).endArray()
                .field("radius","77km")
                .endObject();
            return builder;
        }));

        // test self crossing of circles
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape",
            new Circle(30, 50, 77000))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }
}
