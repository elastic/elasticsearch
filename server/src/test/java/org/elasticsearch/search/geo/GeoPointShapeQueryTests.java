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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.locationtech.jts.geom.Coordinate;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class GeoPointShapeQueryTests extends GeoQueryTests {

    @Override
    protected XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("location")
            .field("type", "geo_point")
            .endObject().endObject().endObject();

        return xcb;
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = Strings.toString(createMapping());
        // where does the mapping ensure geo_shape type for the location field, a template?
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startArray("location").value(-30).value(-30).endArray()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
            .field("name", "Document 2")
            .startArray("location").value(-45).value(-50).endArray()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        // Pending new query processor for lat lon point queries + new code for geo shape field mapper

        /*
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", shape))
                .get();

        assertSearchResponse(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
         */

    }

}
