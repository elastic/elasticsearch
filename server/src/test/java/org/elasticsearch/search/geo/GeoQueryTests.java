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

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.locationtech.jts.geom.Coordinate;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class GeoQueryTests extends ESSingleNodeTestCase {

    protected abstract XContentBuilder createDefaultMapping() throws Exception;

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("aNullshape").setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet("test", "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle(String mapping) throws Exception {
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-30).value(-30).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
            .field("name", "Document 2")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-45).value(-50).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", shape))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setQuery(geoShapeQuery("geo", shape))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

}
