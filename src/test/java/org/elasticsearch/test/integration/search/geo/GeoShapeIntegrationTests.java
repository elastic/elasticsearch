package org.elasticsearch.test.integration.search.geo;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.GeoShapeFilterParser;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.geo.ShapeBuilder.newRectangle;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoShapeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeIntegrationTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testIndexPointsFilterRectangle() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Document 2")
                .startObject("location")
                    .field("type", "point")
                    .startArray("coordinates").value(-45).value(-50).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(),
                        geoShapeFilter("location", shape).relation(ShapeRelation.INTERSECTS)))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));

        searchResponse = client.prepareSearch()
                .setQuery(geoShapeQuery("location", shape).relation(ShapeRelation.INTERSECTS))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
    }
}
