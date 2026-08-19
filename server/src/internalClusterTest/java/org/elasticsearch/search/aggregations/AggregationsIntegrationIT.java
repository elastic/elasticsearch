/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.SuiteScopeTestCase
public class AggregationsIntegrationIT extends ESIntegTestCase {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("index").setMapping("f", "type=keyword").get());
        numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(prepareIndex("index").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
    }

    public void testScroll() {
        final int size = randomIntBetween(1, 4);
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            prepareSearch("index").setSize(size).addAggregation(terms("f").field("f")),
            numDocs,
            (respNum, response) -> {
                assertNoFailures(response);

                if (respNum == 1) { // initial response.
                    InternalAggregations aggregations = response.getAggregations();
                    assertNotNull(aggregations);
                    Terms terms = aggregations.get("f");
                    assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());
                } else {
                    assertNull(response.getAggregations());
                }
            }
        );
    }

    public void testDeeplyNestedAggregationIsRejectedWithoutKillingTheNode() {
        TermsAggregationBuilder agg = terms("a0").field("f");
        for (int i = 1; i < 2000; i++) {
            agg = terms("a" + i).field("f").subAggregation(agg);
        }
        final TermsAggregationBuilder deepAgg = agg;
        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> prepareSearch("index").addAggregation(deepAgg).get()
        );
        assertThat(
            e.getMessage(),
            containsString(
                "The nested depth of the aggregations exceeds the maximum nested depth for aggregations of ["
                    + AggregatorFactories.MAX_NESTED_DEPTH
                    + "]"
            )
        );
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));

        ensureGreen("index");
        assertNoFailures(prepareSearch("index").addAggregation(terms("f").field("f")));
    }

    public void testDeeplyNestedAggregationIsRejectedAsAWholeRequestWithPartialResultsAllowed() {
        assertAcked(
            prepareCreate("deeply-nested-multi-shard").setSettings(indexSettings(between(2, 5), 0)).setMapping("f", "type=keyword")
        );
        indexRandom(true, prepareIndex("deeply-nested-multi-shard").setSource("f", "0"));

        TermsAggregationBuilder agg = terms("a0").field("f");
        for (int i = 1; i < 2000; i++) {
            agg = terms("a" + i).field("f").subAggregation(agg);
        }
        final TermsAggregationBuilder deepAgg = agg;
        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> prepareSearch("deeply-nested-multi-shard").setAllowPartialSearchResults(true).addAggregation(deepAgg).get()
        );
        assertThat(
            e.getMessage(),
            containsString(
                "The nested depth of the aggregations exceeds the maximum nested depth for aggregations of ["
                    + AggregatorFactories.MAX_NESTED_DEPTH
                    + "]"
            )
        );

        ensureGreen("deeply-nested-multi-shard");
        assertNoFailures(prepareSearch("deeply-nested-multi-shard").addAggregation(terms("f").field("f")));
    }
}
