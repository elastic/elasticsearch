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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryFunctionTests;
import org.elasticsearch.index.query.functionscore.QueryFunctionBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.functionscore.FunctionScoreQueryFunctionTests.getQueryFunctionQueryBuilder;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for functions in a function_score query.
 */
public class FunctionScoreIT extends ESIntegTestCase {
    public void testFieldValueFactor() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("test")
                        .field("type", randomFrom(new String[]{"short", "float", "long", "integer", "double"}))
                        .endObject()
                        .startObject("body")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5, "body", "foo").get();
        client().prepareIndex("test", "type1", "2").setSource("test", 17, "body", "foo").get();
        client().prepareIndex("test", "type1", "3").setSource("body", "bar").get();

        refresh();

        // document 2 scores higher because 17 > 5
        SearchResponse response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test")))
                .get();
        assertOrderedSearchHits(response, "2", "1");

        // try again, but this time explicitly use the do-nothing modifier
        response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.NONE)))
                .get();
        assertOrderedSearchHits(response, "2", "1");

        // document 1 scores higher because 1/5 > 1/17
        response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL)))
                .get();
        assertOrderedSearchHits(response, "1", "2");

        // doc 3 doesn't have a "test" field, so an exception will be thrown
        try {
            response = client().prepareSearch("test")
                    .setExplain(randomBoolean())
                    .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("test")))
                    .get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // We are expecting an exception, because 3 has no field
        }

        // doc 3 doesn't have a "test" field but we're defaulting it to 100 so it should be last
        response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(matchAllQuery(),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)))
                .get();
        assertOrderedSearchHits(response, "1", "2", "3");

        // field is not mapped but we're defaulting it to 100 so all documents should have the same score
        response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(matchAllQuery(),
                        fieldValueFactorFunction("notmapped").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)))
                .get();
        assertEquals(response.getHits().getAt(0).score(), response.getHits().getAt(2).score(), 0);


        // n divided by 0 is infinity, which should provoke an exception.
        try {
            response = client().prepareSearch("test")
                    .setExplain(randomBoolean())
                    .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"),
                            fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).factor(0)))
                    .get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // This is fine, the query will throw an exception if executed
            // locally, instead of just having failures
        }
    }

    public void testQueryFunction() throws ExecutionException, InterruptedException {
        indexLimerick();

        FunctionScoreQueryBuilder queryBuilder =  getQueryFunctionQueryBuilder();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(queryBuilder).get();
        assertNoFailures(searchResponse);
        assertSearchHits(searchResponse, "4", "1", "3", "2");
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5f));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(4f));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(3f));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(2f));
    }

    public void indexLimerick() throws InterruptedException, ExecutionException {
        indexRandom(true,
            client().prepareIndex("test", "type", "1")
                .setSource(FunctionScoreQueryFunctionTests.FIELD, FunctionScoreQueryFunctionTests.TEXT_1),
            client().prepareIndex("test", "type", "2")
                .setSource(FunctionScoreQueryFunctionTests.FIELD, FunctionScoreQueryFunctionTests.TEXT_2),
            client().prepareIndex("test", "type", "3")
                .setSource(FunctionScoreQueryFunctionTests.FIELD, FunctionScoreQueryFunctionTests.TEXT_3),
            client().prepareIndex("test", "type", "4")
                .setSource(FunctionScoreQueryFunctionTests.FIELD, FunctionScoreQueryFunctionTests.TEXT_4)
        );
    }

    public void testQueryFunctionSingleFunction() throws ExecutionException, InterruptedException {
        indexLimerick();
        FunctionScoreQueryBuilder queryBuilder =  getQueryFunctionQueryBuilder();
        FunctionScoreQueryBuilder finalQueryBuilder = new FunctionScoreQueryBuilder(new QueryFunctionBuilder(queryBuilder));
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(finalQueryBuilder).get();
        assertNoFailures(searchResponse);
        assertSearchHits(searchResponse, "4", "1", "3", "2");
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5f));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(4f));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(3f));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(2f));
    }
}
