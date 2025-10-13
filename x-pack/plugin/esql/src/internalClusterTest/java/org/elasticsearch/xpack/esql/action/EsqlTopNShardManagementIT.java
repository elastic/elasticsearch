/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Verifies that the TopNOperator can release shard contexts as it processes its input.
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class EsqlTopNShardManagementIT extends AbstractPausableIntegTestCase {
    private static List<SearchContext> searchContexts = new ArrayList<>();
    private static final int SHARD_COUNT = 10;

    @Override
    protected Class<? extends Plugin> pausableFieldPluginClass() {
        return TopNPausableFieldPlugin.class;
    }

    @Override
    protected int shardCount() {
        return SHARD_COUNT;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockSearchService.TestPlugin.class);
    }

    @Before
    public void setupMockService() {
        searchContexts.clear();
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            ((MockSearchService) service).setOnCreateSearchContext(ctx -> {
                searchContexts.add(ctx);
                scriptPermits.release();
            });
        }
    }

    public void testTopNOperatorReleasesContexts() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            var getResultsRequest = new GetAsyncResultRequest(initialResponse.asyncExecutionId().get());
            scriptPermits.release(numberOfDocs());
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(10));
            var result = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).get();
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            result.close();
        }
    }

    private static EsqlQueryResponse sendAsyncQuery() {
        scriptPermits.drainPermits();
        return EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client())
            // Ensures there is no TopN pushdown to lucene, and that the pause happens after the TopN operator has been applied.
            .query("from test | sort foo + 1 | limit 1 | where pause_me + 1 > 42 | stats sum(pause_me)")
            .pragmas(
                new QueryPragmas(
                    Settings.builder()
                        // Configured to ensure that there is only one worker handling all the shards, so that we can assert the correct
                        // expected behavior.
                        .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1)
                        .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), SHARD_COUNT)
                        .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
                        .build()
                )
            )
            .execute()
            .actionGet(1, TimeUnit.MINUTES);
    }

    public static class TopNPausableFieldPlugin extends AbstractPauseFieldPlugin {
        @Override
        protected boolean onWait() throws InterruptedException {
            var acquired = scriptPermits.tryAcquire(SHARD_COUNT, 1, TimeUnit.MINUTES);
            assertTrue("Failed to acquire permits", acquired);
            int closed = 0;
            int open = 0;
            for (SearchContext searchContext : searchContexts) {
                if (searchContext.isClosed()) {
                    closed++;
                } else {
                    open++;
                }
            }
            assertThat(
                Strings.format("most contexts to be closed, but %d were closed and %d were open", closed, open),
                closed,
                greaterThanOrEqualTo(open)
            );
            return scriptPermits.tryAcquire(1, 1, TimeUnit.MINUTES);
        }
    }
}
