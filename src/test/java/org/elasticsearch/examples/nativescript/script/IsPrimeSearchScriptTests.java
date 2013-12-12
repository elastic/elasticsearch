package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

/**
 */
public class IsPrimeSearchScriptTests extends AbstractSearchScriptTests {

    public static int[] PRIMES_10 = new int[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };

    @Test
    public void testIsPrimeScript() throws Exception {
      
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("number").field("type", "integer").endObject()
                .endObject().endObject().endObject()
                .string();
        
        assertAcked(prepareCreate("test")
                .addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index 100 records (0..99)
        for (int i = 0; i < 100; i++) {
            indexBuilders.add(
                    client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .field("number", i)
                            .endObject()));
        }
        // Index a few records with empty number
        for (int i = 100; i < 105; i++) {
            indexBuilders.add(
                    client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .endObject()));
        }

        indexRandom(true, indexBuilders);
        
        // Retrieve first 10 prime records
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), 
                        scriptFilter("is_prime").lang("native").addParam("field", "number")))
                .addField("name")
                .setSize(10)
                .addSort("number", SortOrder.ASC)
                .execute().actionGet();
        
        assertNoFailures(searchResponse);

        // There should be 25 prime numbers between 0 and 100
        assertHitCount(searchResponse, 25);

        // Verify that they are indeed prime numbers
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).field("name").getValue().toString(), equalTo("rec " + PRIMES_10[i]));
        }

        // Check certainty parameter - with certainty == 0, it should return all numbers, but only if numbers are present
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(),
                        scriptFilter("is_prime").lang("native").addParam("field", "number").addParam("certainty", 0)))
                .addField("name")
                .setSize(10)
                .addSort("number", SortOrder.ASC)
                .execute().actionGet();
        assertNoFailures(searchResponse);
        // With certainty 0 no check is done so it should return all numbers
        assertHitCount(searchResponse, 100);

    }

}
