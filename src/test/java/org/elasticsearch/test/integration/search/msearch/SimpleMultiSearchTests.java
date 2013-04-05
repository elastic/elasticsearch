package org.elasticsearch.test.integration.search.msearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.Test;

/**
 */
@Test
public class SimpleMultiSearchTests extends AbstractSharedClusterTest {

    @Test
    public void simpleMultiSearch() {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().prepareIndex("test", "type", "1").setSource("field", "xxx").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "yyy").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        MultiSearchResponse response = client().prepareMultiSearch()
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();

        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getHits().totalHits(), equalTo(1l));
        assertThat(response.getResponses()[1].getResponse().getHits().totalHits(), equalTo(1l));
        assertThat(response.getResponses()[2].getResponse().getHits().totalHits(), equalTo(2l));

        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).id(), equalTo("2"));
    }
}
