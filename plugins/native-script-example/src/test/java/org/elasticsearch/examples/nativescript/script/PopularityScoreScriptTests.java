/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

/**
 */
public class PopularityScoreScriptTests extends AbstractSearchScriptTestCase {

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

        Map<String, Object> params = MapBuilder.<String, Object>newMapBuilder().put("field", "number").map();
        // Retrieve first 10 hits
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.functionScoreQuery(matchQuery("name", "rec"), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script("popularity", ScriptService.ScriptType.INLINE, "native", params)))})
                        .boostMode(CombineFunction.REPLACE))
                .setSize(10)
                .setFetchSource("name", null)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 10 hist
        assertHitCount(searchResponse, 10);

        // Verify that first 5 hits are sorted from 4 to 0
        for (int i = 0; i < 5; i++) {
            assertThat(searchResponse.getHits().getAt(i).sourceAsMap().get("name"), equalTo("rec " + (4 - i)));
        }

        // Verify that hit 5 has non-zero score
        assertThat(searchResponse.getHits().getAt(5).score(), greaterThan(0.0f));

        // Verify that the last 5 hits has the same score
        for (int i = 6; i < 10; i++) {
            assertThat(searchResponse.getHits().getAt(i).score(), equalTo(searchResponse.getHits().getAt(5).score()));
        }
    }
}
