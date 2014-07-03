package org.elasticsearch.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

public class ExpressionScriptTest extends ElasticsearchIntegrationTest {

    @Test
    public void testBasic() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();
        String script = "foo + 2";
        SearchResponse resp = client().prepareSearch("test")
        .setSource("{\"query\": {" +
                       "\"function_score\": {" +
                           "\"query\":{\"match_all\":{}}," +
                           "\"boost_mode\": \"replace\"," +
                           "\"script_score\": {" +
                               "\"script\": \"" + script +"\"," +
                               "\"lang\": \"expression\"" +
                           "}" +
                        "}" +
                    "}}").get();

        assertEquals(7.0, resp.getHits().getAt(0).getScore(), 0.0001f);
    }


}
