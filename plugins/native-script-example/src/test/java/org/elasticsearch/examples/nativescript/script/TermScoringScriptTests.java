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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test if the computed tfidf in NaiveTFIDFScoreScript equals 0.0 for each
 * document as would be expected if each document in the index contains only one
 * and always the same term.
 */
public class TermScoringScriptTests extends AbstractSearchScriptTestCase {

    final static List<String> searchTerms = Arrays.asList("foo", "bar");
    final static String field = "field";
    final static String wordCountField = field + ".word_count";
    final static String placeholder = "placeholder";
    final static List<Double> weights = Arrays.asList(1.0, 1.0);
    final static int numDocs = 100;

    public void testNoOfShardsIs1() {
        assertAcked(prepareCreate("test").get());
        assertThat(client().admin().indices().prepareGetSettings("test").get().getSetting("test", "index.number_of_shards"), equalTo("1"));
    }

    public void testTFIDF() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<>();
        params.put("field", field);
        params.put("terms", searchTerms);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
            .prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script(TFIDFScoreScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params)))})
                .boostMode(CombineFunction.REPLACE)).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(numDocs - i - 1)));
        }
    }

    public void testCosineSimilarity() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<>();
        params.put("field", field);
        params.put("terms", searchTerms);
        params.put("weights", weights);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
            .prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script(CosineSimilarityScoreScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params)))})
                .boostMode(CombineFunction.REPLACE)).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
        }
    }

    public void testPhraseScorer() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<>();
        params.put("field", field);
        params.put("terms", searchTerms);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
            .prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(
                    new Script(PhraseScoreScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params)))})
                .boostMode(CombineFunction.REPLACE)).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
            assertThat((double) hits[i].score(), closeTo(1.0 / (float) (i + 2), 1.e-6));
        }
    }

    public void testLanguageModelScorer() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<>();
        params.put("field", field);
        params.put("terms", searchTerms);
        params.put("word_count_field", wordCountField);
        params.put("lambda", 0.9);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
            .prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script(LanguageModelScoreScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params)))})
                .boostMode(CombineFunction.REPLACE)).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
        }
    }

    private void initData() throws IOException, InterruptedException, ExecutionException {
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject(field).field("type", "string")
            .startObject("fields")
            .startObject("word_count").field("type", "string").field("analyzer", "standard").field("type", "token_count").startObject("fielddata").field("format", "doc_values").endObject().endObject()
            .endObject()
            .endObject().endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        // Index numDocs records (0..99)
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(client().prepareIndex("test", "type", Integer.toString(i))
                .setSource(XContentFactory.jsonBuilder().startObject().field(field, createText(i + 1)).endObject())
                .setId(Integer.toString(i)));
        }
        indexRandom(true, indexBuilders);
    }

    private String createText(int numFoo) {
        String text = "";
        for (int i = 0; i < numFoo; i++) {
            text = text + " foo ";
        }
        for (int i = 0; i < numFoo; i++) {
            text = text + " " + placeholder + " ";
        }
        return text + " bar";
    }
}
