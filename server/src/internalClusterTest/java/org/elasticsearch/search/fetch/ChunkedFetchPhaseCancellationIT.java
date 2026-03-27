/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.AbstractSearchCancellationTestCase;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.test.AbstractSearchCancellationTestCase.ScriptedBlockPlugin.SEARCH_BLOCK_SCRIPT_NAME;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ChunkedFetchPhaseCancellationIT extends AbstractSearchCancellationTestCase {

    @Override
    protected boolean enableConcurrentSearch() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.breaker.request.type", "memory")
            .put("indices.breaker.request.limit", "100mb")
            .put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)
            .build();
    }

    public void testTaskCancellationReleasesCoordinatorBreakerBytes() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        createIndex("test", 2, 0);
        indexTestData();
        ensureGreen("test");

        var plugins = initBlockFactory();
        long breakerBefore = getRequestBreakerUsed(coordinatorNode);

        ActionFuture<SearchResponse> searchResponse = internalCluster().client(coordinatorNode)
            .prepareSearch("test")
            .addScriptField("test_field", new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap()))
            .setAllowPartialSearchResults(true)
            .execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);

        assertBusy(
            () -> assertThat(
                "Coordinator breaker bytes should be released after cancellation",
                getRequestBreakerUsed(coordinatorNode),
                lessThanOrEqualTo(breakerBefore)
            )
        );
    }

    private long getRequestBreakerUsed(String node) {
        CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, node);
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        return breaker.getUsed();
    }
}
