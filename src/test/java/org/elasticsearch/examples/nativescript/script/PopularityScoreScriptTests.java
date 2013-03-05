package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.customScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class PopularityScoreScriptTests extends AbstractSearchScriptTests {

    @Test
    public void testPopularityScoring() throws Exception {
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

        // Index 5 records with non-empty number field
        for (int i = 0; i < 5; i++) {
            node.client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .field("number", i)
                            .endObject())
                    .execute().actionGet();
        }
        // Index a few records with empty number
        for (int i = 5; i < 10; i++) {
            node.client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .endObject())
                    .execute().actionGet();
        }
        node.client().admin().indices().prepareRefresh("test").execute().actionGet();

        // Retrieve first 10 hits
        SearchResponse searchResponse = node.client().prepareSearch("test")
                .setQuery(customScoreQuery(matchQuery("name", "rec"))
                        .script("popularity").lang("native").param("field", "number"))
                .setSize(10)
                .addField("name")
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        // There should be 10 hist
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        // Verify that first 5 hits are sorted from 4 to 0
        for (int i = 0; i < 5; i++) {
            assertThat(searchResponse.getHits().getAt(i).field("name").getValue().toString(), equalTo("rec " + (4 - i)));
        }

        // Verify that hit 5 has non-zero score
        assertThat(searchResponse.getHits().getAt(5).score(), greaterThan(0.0f));

        // Verify that the last 5 hits has the same score
        for (int i = 6; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).score(), equalTo(searchResponse.getHits().getAt(5).score()));
        }
    }
}
