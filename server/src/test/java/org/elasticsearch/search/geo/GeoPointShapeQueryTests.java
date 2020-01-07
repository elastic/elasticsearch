package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.locationtech.jts.geom.Coordinate;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

public class GeoPointShapeQueryTests extends GeoQueryTests {

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = Strings.toString(createMapping());
        // where does the mapping ensure geo_shape type for the location field, a template?
        client().admin().indices().prepareCreate("test").setMapping(createMapping().toString()).get();
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

        // This is deprecated...? go fix
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", shape))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", shape))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    @Override
    protected XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("location")
            .field("type", "geo_point")
            .endObject().endObject().endObject();

        return xcb;
    }

}
