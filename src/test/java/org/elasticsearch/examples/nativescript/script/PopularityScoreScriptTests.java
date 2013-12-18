package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.junit.Test;

/**
 */
public class PopularityScoreScriptTests extends AbstractSearchScriptTests {

    @Test
    public void testPopularityScoring() throws Exception {
      
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
        
        // Index 5 records with non-empty number field
        for (int i = 0; i < 5; i++) {
            indexBuilders.add(
                    client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .field("number", i + 1)
                            .endObject()));
        }
        // Index a few records with empty number
        for (int i = 5; i < 10; i++) {
            indexBuilders.add(
                    client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("name", "rec " + i)
                            .endObject()));
        }
        
        indexRandom(true, indexBuilders);
        
        Map<String, Object> params = MapBuilder.<String, Object> newMapBuilder().put("field", "number").map();
        // Retrieve first 10 hits
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("name", "rec"))
                        .boostMode(CombineFunction.REPLACE)
                        .add(ScoreFunctionBuilders.scriptFunction("popularity", "native", params)))
                .setSize(10)
                .addField("name")
                .execute().actionGet();
        
        assertNoFailures(searchResponse);

        // There should be 10 hist
        assertHitCount(searchResponse, 10);

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
