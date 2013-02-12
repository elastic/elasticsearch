package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class IsPrimeSearchScriptTests extends AbstractSearchScriptTests {

    public static int[] PRIMES_10 = new int[]{2, 3, 5, 7, 11, 13, 17, 19, 23, 29};

    @Test
    public void testIsPrimeScript() throws Exception {
        // Delete the old index
        try {
            node.client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException ex) {
            // Ignore
        }

        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("number").field("type", "integer").endObject()
                .endObject().endObject().endObject()
                .string();
        node.client().admin().indices().prepareCreate("test")
                .addMapping("type", mapping)
                .execute().actionGet();

        // Index 100 records (0..99)
        for (int i = 0; i < 100; i++) {
            node.client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .field("number", i)
                            .endObject())
                    .execute().actionGet();
        }
        // Index a few records with empty number
        for (int i = 100; i < 105; i++) {
            node.client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .endObject())
                    .execute().actionGet();
        }
        node.client().admin().indices().prepareRefresh("test").execute().actionGet();

        // Retrieve first 10 prime records
        SearchResponse searchResponse = node.client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(),
                        scriptFilter("is_prime").lang("native").addParam("field", "number")))
                .addField("name")
                .setSize(10)
                .addSort("number", SortOrder.ASC)
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        // There should be 25 prime numbers between 0 and 100
        assertThat(searchResponse.hits().totalHits(), equalTo(25l));

        // Verify that they are indeed prime numbers
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).field("name").getValue().toString(), equalTo("rec " + PRIMES_10[i]));
        }

        // Check certainty parameter - with certainty == 0, it should return all numbers, but only if numbers are present
        searchResponse = node.client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(),
                        scriptFilter("is_prime").lang("native").addParam("field", "number").addParam("certainty", 0)))
                .addField("name")
                .setSize(10)
                .addSort("number", SortOrder.ASC)
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        // With certainty 0 no check is done so it should return all numbers
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));


    }


}
