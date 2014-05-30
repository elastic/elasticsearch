package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

/**
 * Test if the computed tfidf in NaiveTFIDFScoreScript equals 0.0 for each
 * document as would be expected if each document in the index contains only one
 * and always the same term.
 * 
 */
public class TermScoringScriptTests extends AbstractSearchScriptTests {

    final static String[] searchTerms = { "foo", "bar" };
    final static String field = "field";
    final static String wordCountField = field + ".word_count";
    final static  String placeholder = "placeholder";
    final static Double[] weights = { 1.0, 1.0 };
    final static int numDocs = 100;

    @Test
    public void testNoOfShardsIs1() {
        assertAcked(prepareCreate("test").get());
        assertThat(client().admin().indices().prepareGetSettings("test").get().getSetting("test", "index.number_of_shards"), equalTo("1"));
    }

    @Test
    public void testTFIDF() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("field", field);
        params.put("terms", searchTerms);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery()
                                .add(ScoreFunctionBuilders.scriptFunction(TFIDFScoreScript.SCRIPT_NAME, "native", params))
                                .boostMode(CombineFunction.REPLACE.getName())).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(numDocs - i - 1)));
        }

    }

    @Test
    public void testCosineSimilarity() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("field", field);
        params.put("terms", searchTerms);
        params.put("weights", weights);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery()
                                .add(ScoreFunctionBuilders.scriptFunction(CosineSimilarityScoreScript.SCRIPT_NAME, "native", params))
                                .boostMode(CombineFunction.REPLACE.getName())).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
        }
    }

    @Test
    public void testPhraseScorer() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("field", field);
        params.put("terms", searchTerms);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery()
                                .add(ScoreFunctionBuilders.scriptFunction(PhraseScoreScript.SCRIPT_NAME, "native", params))
                                .boostMode(CombineFunction.REPLACE.getName())).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
            assertThat((double) hits[i].score(), closeTo(1.0 / (float) (i + 2), 1.e-6));
        }
    }

    @Test
    public void testLanguageModelScorer() throws Exception {

        initData();

        // initialize parameters for script
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("field", field);
        params.put("terms", searchTerms);
        params.put("word_count_field", wordCountField);
        params.put("lambda", 0.9);

        // Retrieve records and see if they scored 0.0
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery()
                                .add(ScoreFunctionBuilders.scriptFunction(LanguageModelScoreScript.SCRIPT_NAME, "native", params))
                                .boostMode(CombineFunction.REPLACE.getName())).setSize(numDocs).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < numDocs; i++) {
            assertThat(hits[i].getId(), equalTo(Integer.toString(i)));
        }
    }

    private void initData() throws IOException, InterruptedException, ExecutionException {
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject(field)
                .field("type", "multi_field").startObject("fields").startObject(field).field("type", "String").endObject()
                .startObject("word_count").field("analyzer", "standard").field("type", "token_count").startObject("fielddata")
                .field("format", "doc_values").endObject().endObject().endObject().endObject().endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
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
