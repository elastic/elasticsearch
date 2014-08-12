/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class FunctionScoreTests extends ElasticsearchIntegrationTest {

    @Test
    public void testExplainQueryOnlyOnce() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "float").endObject().endObject().endObject().endObject()));
        ensureYellow();

        client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").field("num", 10).endObject()).get();
        refresh();

        SearchResponse response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(gaussDecayFunction("num", 5, 5)).add(exponentialDecayFunction("num", 5, 5)).add(linearDecayFunction("num", 5, 5))))).get();
        String explanation = response.getHits().getAt(0).explanation().toString();

        checkQueryExplanationAppearsOnlyOnce(explanation);
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(fieldValueFactorFunction("num"))))).get();
        explanation = response.getHits().getAt(0).explanation().toString();
        checkQueryExplanationAppearsOnlyOnce(explanation);

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(randomFunction(10))))).get();
        explanation = response.getHits().getAt(0).explanation().toString();

        checkQueryExplanationAppearsOnlyOnce(explanation);
    }

    private void checkQueryExplanationAppearsOnlyOnce(String explanation) {
        // use some substring of the query explanation and see if it appears twice
        String queryExplanation = "idf(docFreq=1, maxDocs=1)";
        int queryExplanationIndex = explanation.indexOf(queryExplanation, 0);
        assertThat(queryExplanationIndex, greaterThan(-1));
        queryExplanationIndex = explanation.indexOf(queryExplanation, queryExplanationIndex + 1);
        assertThat(queryExplanationIndex, equalTo(-1));
    }

}
