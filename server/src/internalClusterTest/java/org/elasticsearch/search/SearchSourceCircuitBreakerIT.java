/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that the coordinating node charges the request circuit breaker for the retained {@link org.elasticsearch.search.builder.SearchSourceBuilder}
 * before fanning a search out to the shards, and releases the charge when the search finishes. A search whose source
 * exceeds the request breaker limit is rejected with a {@link CircuitBreakingException} naming {@code <search_source>};
 * a subsequent small search then succeeds and the breaker returns to its baseline, proving the charge is released.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SearchSourceCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "search_source_breaker_idx";
    // Mirrors TransportSearchAction.SEARCH_SOURCE_BREAKER_LABEL (package-private, hence hardcoded here).
    private static final String SEARCH_SOURCE_LABEL = "<search_source>";

    public void testLargeSearchSourceTripsRequestBreakerThenReleasesOnSuccess() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).setMapping("field", "type=keyword")
        );
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            docs.add(prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i));
        }
        indexRandom(true, docs);
        ensureGreen(INDEX_NAME);

        long breakerBaseline = getRequestBreakerUsed(coordinatorNode);

        // Shrink the request breaker so the retained source is what trips it, well before any fan-out to shards.
        updateClusterSettings(Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "2kb"));
        try {
            // A terms query with many terms produces a source far larger than the 2kb request limit.
            List<String> manyTerms = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                manyTerms.add("term_" + i + "_" + randomAlphaOfLength(16));
            }
            Exception e = expectThrows(
                Exception.class,
                internalCluster().client(coordinatorNode).prepareSearch(INDEX_NAME).setQuery(new TermsQueryBuilder("field", manyTerms))
            );
            CircuitBreakingException cbe = (CircuitBreakingException) ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
            assertThat("search source charge should trip the request breaker", cbe, notNullValue());
            assertThat(cbe.toString(), containsString(SEARCH_SOURCE_LABEL));
            assertThat(cbe.toString(), containsString("[request] Data too large"));
        } finally {
            updateClusterSettings(
                Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            );
        }

        // The tripped charge reserves nothing, so the breaker is already back at baseline.
        assertBusy(
            () -> assertThat(
                "request breaker must not leak after a tripped search source charge",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBaseline)
            )
        );

        // A small search now succeeds and the breaker returns to baseline, proving the charge is released on completion.
        assertNoFailuresAndResponse(
            internalCluster().client(coordinatorNode).prepareSearch(INDEX_NAME).setQuery(matchAllQuery()).setSize(1),
            response -> assertThat(response.getHits().getHits().length, equalTo(1))
        );
        assertBusy(
            () -> assertThat(
                "request breaker must be released after a successful search",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBaseline)
            )
        );
    }

    private long getRequestBreakerUsed(String node) {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, node);
        return breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed();
    }
}
