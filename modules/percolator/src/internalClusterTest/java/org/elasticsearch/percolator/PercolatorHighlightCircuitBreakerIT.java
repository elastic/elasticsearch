/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numClientNodes = 0, maxNumDataNodes = 1)
public class PercolatorHighlightCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "percolator-highlight-cb-test";
    private static final String FIELD = "field1";
    private static final String TERM = "abcdefghij";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(PercolatorPlugin.class);
    }

    @Before
    public void checkBreakerType() {
        assumeFalse("--> noop breakers used, skipping test", noopBreakerUsed());
    }

    public void testPercolateFuzzyHighlightingReleasesBreakerMemory() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX_NAME).setMapping("id", "type=keyword", FIELD, "type=text", "query", "type=percolator")
        );
        prepareIndex(INDEX_NAME).setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", fuzzyQuery(FIELD, TERM).fuzziness(Fuzziness.TWO).prefixLength(0))
                    .endObject()
            )
            .get();
        refresh(INDEX_NAME);

        assertBusy(() -> assertEquals("Request breaker should be empty before search", 0L, getRequestBreakerEstimated()));

        BytesReference document = BytesReference.bytes(jsonBuilder().startObject().field(FIELD, "some " + TERM + " content").endObject());
        assertResponse(
            prepareSearch(INDEX_NAME).setQuery(new PercolateQueryBuilder("query", document, XContentType.JSON))
                .highlighter(new HighlightBuilder().field(FIELD)),
            response -> assertEquals(1L, response.getHits().getTotalHits().value())
        );

        assertBusy(
            () -> assertEquals("Request breaker memory should be released after search completes", 0L, getRequestBreakerEstimated())
        );
    }

    public void testMultiplePercolateQueriesPerHitReleaseBreakerMemory() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX_NAME).setMapping("id", "type=keyword", FIELD, "type=text", "query", "type=percolator")
        );
        prepareIndex(INDEX_NAME).setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", fuzzyQuery(FIELD, TERM).fuzziness(Fuzziness.TWO).prefixLength(0))
                    .endObject()
            )
            .get();
        refresh(INDEX_NAME);

        assertBusy(() -> assertEquals("Request breaker should be empty before search", 0L, getRequestBreakerEstimated()));

        BytesReference document1 = BytesReference.bytes(jsonBuilder().startObject().field(FIELD, "some " + TERM + " content").endObject());
        BytesReference document2 = BytesReference.bytes(jsonBuilder().startObject().field(FIELD, "other " + TERM + " stuff").endObject());
        assertResponse(
            prepareSearch(INDEX_NAME).setQuery(
                boolQuery().should(new PercolateQueryBuilder("query", document1, XContentType.JSON).setName("query1"))
                    .should(new PercolateQueryBuilder("query", document2, XContentType.JSON).setName("query2"))
            ).highlighter(new HighlightBuilder().field(FIELD)),
            response -> {
                assertEquals(1L, response.getHits().getTotalHits().value());
                assertEquals(2, response.getHits().getAt(0).getHighlightFields().size());
            }
        );

        assertBusy(
            () -> assertEquals("Request breaker memory should be released after search completes", 0L, getRequestBreakerEstimated())
        );
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
}
