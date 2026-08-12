/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.containsString;

@ESIntegTestCase.ClusterScope(scope = TEST, numClientNodes = 0, maxNumDataNodes = 1)
public class HighlightAutomatonCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "highlight-cb-test";
    private static final String KEYWORD_FIELD = "kw_field";
    private static final String TERM = "abcdefghij";

    @Before
    public void checkBreakerType() {
        assumeFalse("--> noop breakers used, skipping test", noopBreakerUsed());
    }

    @After
    public void resetBreakerSettings() {
        updateClusterSettings(
            Settings.builder()
                .putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
                .putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey())
        );
    }

    public void testHighlightedFuzzyQueryReleasesBreakerMemory() throws Exception {
        createAndPopulateIndex();
        assertBusy(() -> assertEquals("Request breaker should be empty before search", 0L, getRequestBreakerEstimated()));

        assertResponse(
            prepareSearch(INDEX_NAME).setQuery(fuzzyQuery())
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field(KEYWORD_FIELD))),
            response -> assertEquals(1L, response.getHits().getTotalHits().value())
        );

        assertBusy(
            () -> assertEquals("Request breaker memory should be released after search completes", 0L, getRequestBreakerEstimated())
        );
    }

    public void testHighlightingTripsCircuitBreakerWhileQueryAloneSucceeds() {
        createAndPopulateIndex();

        FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term(KEYWORD_FIELD, TERM), Fuzziness.TWO.asDistance(TERM), 0);
        long automataBytes = FuzzyQueries.estimateAutomataBytes(fuzzyQuery);
        long limitBytes = automataBytes + automataBytes / 2;

        updateClusterSettings(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes + "b")
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
        );

        assertResponse(
            prepareSearch(INDEX_NAME).setQuery(fuzzyQuery()),
            response -> assertEquals(1L, response.getHits().getTotalHits().value())
        );

        assertFailures(
            prepareSearch(INDEX_NAME).setQuery(fuzzyQuery())
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field(KEYWORD_FIELD))),
            RestStatus.TOO_MANY_REQUESTS,
            containsString("Data too large")
        );
    }

    public void testMatchedFieldsScalesHighlightCharge() {
        createAndPopulateIndex();

        FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term(KEYWORD_FIELD, TERM), Fuzziness.TWO.asDistance(TERM), 0);
        long queryBytes = FuzzyQueries.estimateBytes(fuzzyQuery);
        long automataBytes = FuzzyQueries.estimateAutomataBytes(fuzzyQuery);
        long limitBytes = queryBytes + automataBytes + automataBytes / 2;

        updateClusterSettings(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes + "b")
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
        );

        assertResponse(
            prepareSearch(INDEX_NAME).setQuery(fuzzyQuery())
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field(KEYWORD_FIELD))),
            response -> assertEquals(1L, response.getHits().getTotalHits().value())
        );

        assertFailures(
            prepareSearch(INDEX_NAME).setQuery(fuzzyQuery())
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field(KEYWORD_FIELD).matchedFields(KEYWORD_FIELD))),
            RestStatus.TOO_MANY_REQUESTS,
            containsString("Data too large")
        );
    }

    private FuzzyQueryBuilder fuzzyQuery() {
        return QueryBuilders.fuzzyQuery(KEYWORD_FIELD, TERM).fuzziness(Fuzziness.TWO).prefixLength(0);
    }

    private boolean noopBreakerUsed() {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        for (NodeStats nodeStats : stats.getNodes()) {
            if (nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
        }
        return false;
    }

    private long getRequestBreakerEstimated() {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        long estimated = 0;
        for (NodeStats stat : stats.getNodes()) {
            CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.REQUEST);
            if (breakerStats != null) {
                estimated += breakerStats.getEstimated();
            }
        }
        return estimated;
    }

    private void createAndPopulateIndex() {
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(KEYWORD_FIELD, "type=keyword")
        );
        prepareIndex(INDEX_NAME).setId("1").setSource(KEYWORD_FIELD, TERM).get();
        for (int i = 0; i < 20; i++) {
            prepareIndex(INDEX_NAME).setId("other-" + i).setSource(KEYWORD_FIELD, "unrelated" + i).get();
        }
        refresh(INDEX_NAME);
    }
}
