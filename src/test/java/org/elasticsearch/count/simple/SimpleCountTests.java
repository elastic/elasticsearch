package org.elasticsearch.count.simple;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class SimpleCountTests extends ElasticsearchIntegrationTest {

    @Test
    public void testCountRandomPreference() throws InterruptedException, ExecutionException {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", between(1, 3))).get();
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("field", "value"),
                client().prepareIndex("test", "type", "2").setSource("field", "value"),
                client().prepareIndex("test", "type", "3").setSource("field", "value"),
                client().prepareIndex("test", "type", "4").setSource("field", "value"),
                client().prepareIndex("test", "type", "5").setSource("field", "value"),
                client().prepareIndex("test", "type", "6").setSource("field", "value"));

        int iters = atLeast(10);
        for (int i = 0; i < iters; i++) {
            // id is not indexed, but lets see that we automatically convert to
            CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomUnicodeOfLengthBetween(0, 4)).get();
            assertHitCount(countResponse, 6l);
        }
    }

    @Test
    public void simpleIpTests() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("from").field("type", "ip").endObject()
                        .startObject("to").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefresh(true).execute().actionGet();

        CountResponse countResponse = client().prepareCount()
                .setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7")))
                .execute().actionGet();

        assertHitCount(countResponse, 1l);
    }

    @Test
    public void simpleIdTests() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefresh(true).execute().actionGet();
        // id is not indexed, but lets see that we automatically convert to
        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.queryString("_id:XXX1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        // id is not index, but we can automatically support prefix as well
        countResponse = client().prepareCount().setQuery(QueryBuilders.prefixQuery("_id", "XXX")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.queryString("_id:XXX*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void simpleDateMathTests() throws Exception {
        prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        ensureGreen();
        refresh();
        CountResponse countResponse = client().prepareCount("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d")).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test").setQuery(QueryBuilders.queryString("field:[2010-01-03||+2d TO 2010-01-04||+2d]")).execute().actionGet();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void localDependentDateTests() throws Exception {
        prepareCreate("test")
                .addMapping("type1",
                        jsonBuilder().startObject()
                                .startObject("type1")
                                .startObject("properties")
                                .startObject("date_field")
                                .field("type", "date")
                                .field("format", "E, d MMM yyyy HH:mm:ss Z")
                                .field("locale", "de")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject())
                .execute().actionGet();
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", "" + i).setSource("date_field", "Mi, 06 Dez 2000 02:55:00 -0800").execute().actionGet();
            client().prepareIndex("test", "type1", "" + (10 + i)).setSource("date_field", "Do, 07 Dez 2000 02:55:00 -0800").execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Do, 07 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(countResponse, 10l);


            countResponse = client().prepareCount("test")
                    .setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Fr, 08 Dez 2000 00:00:00 -0800"))
                    .execute().actionGet();
            assertHitCount(countResponse, 20l);

        }
    }
}
