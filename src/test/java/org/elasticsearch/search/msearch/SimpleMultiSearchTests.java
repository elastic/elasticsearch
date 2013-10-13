package org.elasticsearch.search.msearch;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SimpleMultiSearchTests extends AbstractIntegrationTest {

    @Test
    public void simpleMultiSearch() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("field", "xxx").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "yyy").execute().actionGet();
        refresh();
        MultiSearchResponse response = client().prepareMultiSearch()
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        
        for (MultiSearchResponse.Item item : response) {
           assertNoFailures(item.getResponse());
        }
        assertThat(response.getResponses().length, equalTo(3));
        assertHitCount(response.getResponses()[0].getResponse(), 1l);
        assertHitCount(response.getResponses()[1].getResponse(), 1l);
        assertHitCount(response.getResponses()[2].getResponse(), 2l);
        assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
        assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
    }
}
