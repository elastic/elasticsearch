/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Tests retrying a failed reindex operation
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RetryFailedReindexIT extends ESIntegTestCase {
    private static final String INDEX = "source-index";
    private static final String DEST_INDEX = "dest-index";
    private static final int NUM_DOCS = 100;
    private static final int NUM_PARTIAL_DOCS = 70;
    private static final AtomicBoolean FILTER_ENABLED = new AtomicBoolean(false);
    private static final AtomicInteger DOC_COUNT = new AtomicInteger(0);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, TestPlugin.class);
    }

    @Before
    public void reset() {
        FILTER_ENABLED.set(false);
        DOC_COUNT.set(0);
    }

    public void testRetryFailedReindex() throws Exception {
        createIndex(INDEX);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, NUM_DOCS)
                .mapToObj(i -> prepareIndex(INDEX).setId(Integer.toString(i)).setSource("n", Integer.toString(i)))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), NUM_DOCS);

        // Fail reindex and end up in partial state
        FILTER_ENABLED.set(true);
        assertFutureThrows(reindex(true), TestException.class);
        FILTER_ENABLED.set(false);

        // Run into conflicts with partial destination index
        assertResponse(reindex(true), res -> {
            assertThat(res.getBulkFailures(), not(empty()));
            for (BulkItemResponse.Failure failure : res.getBulkFailures()) {
                assertThat(failure.getMessage(), containsString("VersionConflictEngineException: ["));
            }
        });

        // Bypass conflicts and complete reindex
        assertResponse(reindex(false), res -> { assertThat(res.getBulkFailures(), empty()); });
        assertBusy(() -> { assertHitCount(prepareSearch(DEST_INDEX).setSize(0).setTrackTotalHits(true), NUM_DOCS); });
    }

    private ActionFuture<BulkByScrollResponse> reindex(boolean abortOnVersionConflict) {
        ReindexRequestBuilder builder = new ReindexRequestBuilder(internalCluster().client());
        builder.source(INDEX).destination(DEST_INDEX).abortOnVersionConflict(abortOnVersionConflict);
        builder.source().setSize(1);
        builder.destination().setOpType(CREATE);
        return builder.execute();
    }

    private static class TestException extends ElasticsearchException {
        TestException() {
            super("Injected index failure");
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (FILTER_ENABLED.get()
                        && action.equals("indices:data/write/bulk")
                        && DOC_COUNT.incrementAndGet() > NUM_PARTIAL_DOCS) {
                        listener.onFailure(new TestException());
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }

            });
        }
    }
}
