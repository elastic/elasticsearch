package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.sort.SortBuilders;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 */
public class RandomSortScriptTests extends AbstractSearchScriptTests {
    @Test
    public void testPseudoRandomScript() throws Exception {
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
                            .endObject())
                    .execute().actionGet();
        }
        node.client().admin().indices().prepareRefresh("test").execute().actionGet();


        // Retrieve first 10 records
        SearchResponse searchResponse = node.client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native").setParams(MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        // Check that random order was applied
        assertThat(searchResponse.getHits().getAt(0).field("name").getValue().toString(), not(equalTo("rec0")));

        String[] records = new String[10];

        // Store sort order
        for (int i = 0; i < 10; i++) {
            records[i] = searchResponse.getHits().getAt(i).field("name").getValue().toString();
        }

        // Retrieve first 10 records again
        searchResponse = node.client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native").setParams(MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        // Verify the same sort order
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).field("name").getValue().toString(), equalTo(records[i]));
        }

        // Retrieve first 10 records without salt
        searchResponse = node.client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        // Verify different sort order
        boolean different = false;
        for (int i = 0; i < 10; i++) {
            if (!records[i].equals(searchResponse.getHits().getAt(i).field("name").getValue().toString())) {
                different = true;
                break;
            }
        }
        assertThat(different, equalTo(true));

    }
}
