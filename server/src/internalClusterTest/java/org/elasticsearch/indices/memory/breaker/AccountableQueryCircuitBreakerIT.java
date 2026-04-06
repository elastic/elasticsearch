/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.memory.breaker;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests verifying that Lucene queries implementing {@link org.apache.lucene.util.Accountable}
 * (wildcard, prefix, regexp, query_string, range) are properly tracked by the request circuit breaker
 * during query construction, and that the accounted memory is released after the search completes.
 */
@ClusterScope(scope = TEST, numClientNodes = 0, maxNumDataNodes = 1)
public class AccountableQueryCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "cb-accountable-test";
    private static final String TEXT_FIELD = "text_field";
    private static final String KEYWORD_FIELD = "keyword_field";

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

    public void testWildcardQueryTripsCircuitBreaker() {
        assertBoolQueryTripsBreaker(100, i -> new WildcardQueryBuilder(TEXT_FIELD, "*pattern*" + i + "*"));
    }

    public void testPrefixQueryTripsCircuitBreaker() {
        assertBoolQueryTripsBreaker(100, i -> new PrefixQueryBuilder(TEXT_FIELD, "prefix" + i));
    }

    public void testRegexpQueryTripsCircuitBreaker() {
        assertBoolQueryTripsBreaker(50, i -> new RegexpQueryBuilder(TEXT_FIELD, "(pattern" + i + "|alternate" + i + "|option" + i + ").*"));
    }

    public void testQueryStringTripsCircuitBreaker() {
        assertBoolQueryTripsBreaker(100, i -> new QueryStringQueryBuilder("*pattern" + i + "*").defaultField(TEXT_FIELD));
    }

    public void testRangeQueryTripsCircuitBreaker() {
        assertBoolQueryTripsBreaker(100, i -> new RangeQueryBuilder(KEYWORD_FIELD).gte("key" + i).lte("key" + (i + 100)));
    }

    public void testWildcardQueryMemoryReleased() throws Exception {
        assertQueryMemoryReleasedAfterSearch(new WildcardQueryBuilder(TEXT_FIELD, "*test*pattern*"));
    }

    public void testPrefixQueryMemoryReleased() throws Exception {
        assertQueryMemoryReleasedAfterSearch(new PrefixQueryBuilder(TEXT_FIELD, "value"));
    }

    public void testRegexpQueryMemoryReleased() throws Exception {
        assertQueryMemoryReleasedAfterSearch(new RegexpQueryBuilder(TEXT_FIELD, ".*test.*pattern.*"));
    }

    public void testQueryStringMemoryReleased() throws Exception {
        assertQueryMemoryReleasedAfterSearch(new QueryStringQueryBuilder("*test*pattern*").defaultField(TEXT_FIELD));
    }

    public void testRangeQueryMemoryReleased() throws Exception {
        assertQueryMemoryReleasedAfterSearch(new RangeQueryBuilder(KEYWORD_FIELD).gte("key0").lte("key999"));
    }

    private void assertBoolQueryTripsBreaker(int count, IntFunction<QueryBuilder> queryFactory) {
        createAndPopulateIndex();

        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (int i = 0; i < count; i++) {
            boolQuery.should(queryFactory.apply(i));
        }
        assertQueryTripsBreaker(boolQuery);
    }

    private void assertQueryMemoryReleasedAfterSearch(QueryBuilder query) throws Exception {
        createAndPopulateIndex();
        assertQueryMemoryReleased(query);
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

    private long getRequestBreakerTrippedCount() {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        long tripped = 0;
        for (NodeStats stat : stats.getNodes()) {
            CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.REQUEST);
            if (breakerStats != null) {
                tripped += breakerStats.getTrippedCount();
            }
        }
        return tripped;
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
            prepareCreate(INDEX_NAME, 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))).setMapping(
                TEXT_FIELD,
                "type=text",
                KEYWORD_FIELD,
                "type=keyword"
            )
        );
        int docCount = scaledRandomIntBetween(100, 500);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(
                client().prepareIndex(INDEX_NAME).setId(Long.toString(id)).setSource(TEXT_FIELD, "value" + id, KEYWORD_FIELD, "key" + id)
            );
        }
        indexRandom(true, false, true, reqs);
    }

    private void assertQueryTripsBreaker(QueryBuilder query) {
        updateClusterSettings(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
        );

        SearchRequestBuilder searchRequest = client().prepareSearch(INDEX_NAME).setQuery(query);
        assertFailures(searchRequest, RestStatus.BAD_REQUEST, containsString("Data too large"));
        assertThat("Request circuit breaker should have tripped", getRequestBreakerTrippedCount(), greaterThanOrEqualTo(1L));
    }

    private void assertQueryMemoryReleased(QueryBuilder query) throws Exception {
        long baseline = getRequestBreakerEstimated();
        client().prepareSearch(INDEX_NAME).setQuery(query).get().decRef();
        assertBusy(() -> {
            long estimated = getRequestBreakerEstimated();
            assertEquals("Request breaker memory should be released after search completes", baseline, estimated);
        });
    }
}
