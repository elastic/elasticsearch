package org.yipeng.client;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by yipeng on 2018/10/7.
 */
public class TestEsSearch {

    public static void main(String[] args) throws IOException {
        expressionSearch();
    }


    private static void expressionSearch() throws IOException {
        RestClientBuilder restClientBuilder = getRestClientBuilder();
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        SearchRequest searchRequest = new SearchRequest("wares");
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "SENNHEISER");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(100);

        Script script = new Script(ScriptType.INLINE, "expression", "doc['s1'].value+doc['s2'].value", Collections.emptyMap());
        ScriptScoreFunctionBuilder scriptBuilder = ScoreFunctionBuilders.scriptFunction(script);

        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(termQueryBuilder, scriptBuilder);   /**{@link org.elasticsearch.common.lucene.search.function.ScriptScoreFunction#getLeafScoreFunction}中调用
                                                                                                                                {@link org.elasticsearch.script.ScoreScript.LeafFactory}实例化**/
        functionScoreQueryBuilder.boostMode(CombineFunction.SUM);
        searchSourceBuilder.query(functionScoreQueryBuilder);
        searchRequest.source(searchSourceBuilder);                  /**{@link org.elasticsearch.search.SearchService#parseSource(DefaultSearchContext, SearchSourceBuilder)}
                                                                        会将source转换为query等信息。这里如果用到了Script,会将调用{@link org.elasticsearch.script.ScriptService#getEngine(String)} 获取对应的script插件}**/

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);  /**查询首先执行{@link org.elasticsearch.search.SearchService#executeQueryPhase},
                                                                                                            然后就是{@link org.elasticsearch.search.query.QueryPhase#execute(SearchContext)}**/

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getScore() +" --- " +searchHit.getSourceAsString());
        }

        restHighLevelClient.close();
    }


    public static RestClientBuilder getRestClientBuilder() {
        HttpHost httpHost = new HttpHost("127.0.0.1", 9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        return restClientBuilder;
    }
}
