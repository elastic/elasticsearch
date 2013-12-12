package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortBuilders;
import org.junit.Test;

/**
 */
public class RandomSortScriptTests extends AbstractSearchScriptTests {
    @Test
    public void testPseudoRandomScript() throws Exception {
      
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
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
                            .endObject()));
        }

        indexRandom(true, indexBuilders);

        // Retrieve first 10 records
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native").setParams(MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()))
                .execute().actionGet();
        
        assertNoFailures(searchResponse);

        // Check that random order was applied
        assertThat(searchResponse.getHits().getAt(0).field("name").getValue().toString(), not(equalTo("rec0")));

        String[] records = new String[10];

        // Store sort order
        for (int i = 0; i < 10; i++) {
            records[i] = searchResponse.getHits().getAt(i).field("name").getValue().toString();
        }

        // Retrieve first 10 records again
        searchResponse = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native").setParams(MapBuilder.<String, Object>newMapBuilder().put("salt", "1234").map()))
                .execute().actionGet();
        
        assertNoFailures(searchResponse);

        // Verify the same sort order
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).field("name").getValue().toString(), equalTo(records[i]));
        }

        // Retrieve first 10 records without salt
        searchResponse = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addField("name")
                .setSize(10)
                .addSort(SortBuilders.scriptSort("random", "number").lang("native"))
                .execute().actionGet();
        
        assertNoFailures(searchResponse);

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
