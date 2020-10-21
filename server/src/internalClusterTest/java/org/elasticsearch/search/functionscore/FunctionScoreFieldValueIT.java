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
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreFieldValueIT extends ESIntegTestCase {
    public void testFieldValueFactor() throws IOException {
        assertAcked(prepareCreate("test").setMapping(
                jsonBuilder()
                        .startObject()
                        .startObject("_doc")
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

        client().prepareIndex("test").setId("1").setSource("test", 5, "body", "foo").get();
        client().prepareIndex("test").setId("2").setSource("test", 17, "body", "foo").get();
        client().prepareIndex("test").setId("3").setSource("body", "bar").get();

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
        assertEquals(response.getHits().getAt(0).getScore(), response.getHits().getAt(2).getScore(), 0);


        client().prepareIndex("test").setId("2").setSource("test", -1, "body", "foo").get();
        refresh();

        // -1 divided by 0 is infinity, which should provoke an exception.
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
}
