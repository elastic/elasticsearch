package org.elasticsearch.test.integration.search.msearch;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class SimpleMultiSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        startNode("node1", settings);
        startNode("node2", settings);
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void simpleMultiSearch() {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.prepareIndex("test", "type", "1").setSource("field", "xxx").execute().actionGet();
        client.prepareIndex("test", "type", "2").setSource("field", "yyy").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        MultiSearchResponse response = client.prepareMultiSearch()
                .add(client.prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(client.prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(client.prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();

        assertThat(response.responses().length, equalTo(3));
        assertThat(response.responses()[0].response().hits().totalHits(), equalTo(1l));
        assertThat(response.responses()[1].response().hits().totalHits(), equalTo(1l));
        assertThat(response.responses()[2].response().hits().totalHits(), equalTo(2l));

        assertThat(response.responses()[0].response().hits().getAt(0).id(), equalTo("1"));
        assertThat(response.responses()[1].response().hits().getAt(0).id(), equalTo("2"));
    }
}
