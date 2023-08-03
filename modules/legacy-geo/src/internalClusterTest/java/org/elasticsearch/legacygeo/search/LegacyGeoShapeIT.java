/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.legacygeo.test.TestLegacyGeoShapeFieldMapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGeoShapeIT extends GeoShapeIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(TestLegacyGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void getGeoShapeMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
        b.field("strategy", "recursive");
    }

    @Override
    protected IndexVersion randomSupportedVersion() {
        return IndexVersionUtils.randomCompatibleVersion(random());
    }

    @Override
    protected boolean allowExpensiveQueries() {
        return false;
    }

    /**
     * Test that the circle is still supported for the legacy shapes
     */
    public void testLegacyCircle() throws Exception {
        // create index
        assertAcked(
            prepareCreate("test").setSettings(settings(randomSupportedVersion()).build())
                .setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash")
                .get()
        );
        ensureGreen();

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
            builder.startObject()
                .field("type", "circle")
                .startArray("coordinates")
                .value(30)
                .value(50)
                .endArray()
                .field("radius", "77km")
                .endObject();
            return builder;
        }));

        // test self crossing of circles
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }
}
