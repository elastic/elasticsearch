/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase()
public class LargeTopHitsIT extends ESIntegTestCase {

    private static final String TERMS_AGGS_FIELD_1 = "terms1";
    private static final String TERMS_AGGS_FIELD_2 = "terms2";
    private static final String TERMS_AGGS_FIELD_3 = "terms3";
    private static final String SORT_FIELD = "sort";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("indices.breaker.request.type", "memory").build();
    }

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        initSmallIdx();
        ensureSearchable();
    }

    private void initSmallIdx() throws IOException {
        createIndex("small_idx");
        ensureGreen("small_idx");
        populateIndex("small_idx", 5, 40_000);
    }

    private void initLargeIdx() throws IOException {
        createIndex("large_idx");
        ensureGreen("large_idx");
        populateIndex("large_idx", 70, 50_000);
    }

    public void testSimple() {
        assertNoFailuresAndResponse(query("small_idx"), response -> {
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
        });
    }

    public void test500Queries() {
        for (int i = 0; i < 500; i++) {
            // make sure we are not leaking memory over multiple queries
            assertNoFailuresAndResponse(query("small_idx"), response -> {
                Terms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
            });
        }
    }

    // This works most of the time, but it's not consistent: it still triggers OOM sometimes.
    // The test env is too small and non-deterministic to hold all these data and results.
    @AwaitsFix(bugUrl = "see comment above")
    public void testBreakAndRecover() throws IOException {
        initLargeIdx();
        assertNoFailuresAndResponse(query("small_idx"), response -> {
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
        });

        assertFailures(query("large_idx"), RestStatus.TOO_MANY_REQUESTS, containsString("Data too large"));

        assertNoFailuresAndResponse(query("small_idx"), response -> {
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
        });
    }

    private void createIndex(String idxName) {
        assertAcked(
            prepareCreate(idxName).setMapping(
                TERMS_AGGS_FIELD_1,
                "type=keyword",
                TERMS_AGGS_FIELD_2,
                "type=keyword",
                TERMS_AGGS_FIELD_3,
                "type=keyword",
                "text",
                "type=text,store=true",
                "large_text_1",
                "type=text,store=false",
                "large_text_2",
                "type=text,store=false",
                "large_text_3",
                "type=text,store=false",
                "large_text_4",
                "type=text,store=false",
                "large_text_5",
                "type=text,store=false"
            )
        );
    }

    private void populateIndex(String idxName, int nDocs, int size) throws IOException {
        for (int i = 0; i < nDocs; i++) {
            List<IndexRequestBuilder> builders = new ArrayList<>();
            builders.add(
                prepareIndex(idxName).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(TERMS_AGGS_FIELD_1, "val" + i % 53)
                            .field(TERMS_AGGS_FIELD_2, "val" + i % 23)
                            .field(TERMS_AGGS_FIELD_3, "val" + i % 10)
                            .field(SORT_FIELD, i)
                            .field("text", "some text to entertain")
                            .field("large_text_1", Strings.repeat("this is a text field 1 ", size))
                            .field("large_text_2", Strings.repeat("this is a text field 2 ", size))
                            .field("large_text_3", Strings.repeat("this is a text field 3 ", size))
                            .field("large_text_4", Strings.repeat("this is a text field 4 ", size))
                            .field("large_text_5", Strings.repeat("this is a text field 5 ", size))
                            .field("field1", 5)
                            .field("field2", 2.71)
                            .endObject()
                    )
            );

            indexRandom(true, builders);
        }
    }

    private static SearchRequestBuilder query(String indexName) {
        return prepareSearch(indexName).addAggregation(
            terms("terms").executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD_1)
                .subAggregation(
                    terms("terms").executionHint(randomExecutionHint())
                        .field(TERMS_AGGS_FIELD_2)
                        .subAggregation(
                            terms("terms").executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD_2)
                                .subAggregation(topHits("hits").sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC)))
                        )
                )
        );
    }
}
