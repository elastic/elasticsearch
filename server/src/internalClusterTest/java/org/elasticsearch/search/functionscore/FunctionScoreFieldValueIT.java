/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreFieldValueIT extends ESIntegTestCase {
    public void testFieldValueFactor() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", randomFrom(new String[] { "short", "float", "long", "integer", "double" }))
                    .endObject()
                    .startObject("body")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        prepareIndex("test").setId("1").setSource("test", 5, "body", "foo").get();
        prepareIndex("test").setId("2").setSource("test", 17, "body", "foo").get();
        prepareIndex("test").setId("3").setSource("body", "bar").get();

        refresh();

        // document 2 scores higher because 17 > 5
        assertOrderedSearchHits(
            prepareSearch("test").setExplain(randomBoolean())
                .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test"))),
            "2",
            "1"
        );

        // try again, but this time explicitly use the do-nothing modifier
        assertOrderedSearchHits(
            prepareSearch("test").setExplain(randomBoolean())
                .setQuery(
                    functionScoreQuery(
                        simpleQueryStringQuery("foo"),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.NONE)
                    )
                ),
            "2",
            "1"
        );

        // document 1 scores higher because 1/5 > 1/17
        assertOrderedSearchHits(
            prepareSearch("test").setExplain(randomBoolean())
                .setQuery(
                    functionScoreQuery(
                        simpleQueryStringQuery("foo"),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL)
                    )
                ),
            "1",
            "2"
        );

        // doc 3 doesn't have a "test" field, so an exception will be thrown
        try {
            assertResponse(
                prepareSearch("test").setExplain(randomBoolean())
                    .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("test"))),
                ElasticsearchAssertions::assertFailures
            );
        } catch (SearchPhaseExecutionException e) {
            // We are expecting an exception, because 3 has no field
        }

        // doc 3 doesn't have a "test" field but we're defaulting it to 100 so it should be last
        assertOrderedSearchHits(
            prepareSearch("test").setExplain(randomBoolean())
                .setQuery(
                    functionScoreQuery(
                        matchAllQuery(),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)
                    )
                ),
            "1",
            "2",
            "3"
        );

        // field is not mapped but we're defaulting it to 100 so all documents should have the same score
        assertResponse(
            prepareSearch("test").setExplain(randomBoolean())
                .setQuery(
                    functionScoreQuery(
                        matchAllQuery(),
                        fieldValueFactorFunction("notmapped").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)
                    )
                ),
            response -> assertEquals(response.getHits().getAt(0).getScore(), response.getHits().getAt(2).getScore(), 0)
        );

        prepareIndex("test").setId("2").setSource("test", -1, "body", "foo").get();
        refresh();

        // -1 divided by 0 is infinity, which should provoke an exception.
        try {
            assertResponse(
                prepareSearch("test").setExplain(randomBoolean())
                    .setQuery(
                        functionScoreQuery(
                            simpleQueryStringQuery("foo"),
                            fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).factor(0)
                        )
                    ),
                ElasticsearchAssertions::assertFailures
            );
        } catch (SearchPhaseExecutionException e) {
            // This is fine, the query will throw an exception if executed
            // locally, instead of just having failures
        }
    }
}
