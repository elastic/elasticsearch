/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexResolverTests extends ESTestCase {

    /**
     * The field caps response completes on another thread, so the resolver must carry the dispatching query's planning
     * CPU metering over to it: merging the mappings, and the listener it completes inline, run inside that query's
     * measurement.
     */
    public void testResponseIsMeteredAsPlanningCpuOfTheDispatchingQuery() {
        assumeTrue("thread CPU time unsupported", ThreadCpuTimer.currentNanos() >= 0);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            FieldCapabilitiesResponse fieldCaps = FieldCapabilitiesResponse.builder()
                .withIndexResponses(
                    List.of(
                        new FieldCapabilitiesIndexResponse(
                            "idx",
                            "idx",
                            Map.of("foo", new IndexFieldCapabilitiesBuilder("foo", "integer").build()),
                            true,
                            IndexMode.STANDARD,
                            0,
                            0,
                            0
                        )
                    )
                )
                .build();
            NoOpClient client = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertSame(EsqlResolveFieldsAction.TYPE, action);
                    // As in production, the response arrives on the search coordination pool, where mergedMappings must run.
                    threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION)
                        .execute(() -> listener.onResponse((Response) new EsqlResolveFieldsResponse(fieldCaps)));
                }
            };
            PlanningCpuTracker tracker = new PlanningCpuTracker();
            PlainActionFuture<Boolean> meteredAtCompletion = new PlainActionFuture<>();
            tracker.meteredCpu(
                () -> new IndexResolver(client, () -> true).resolveLookupIndices(
                    "idx",
                    Set.of("foo"),
                    TransportVersion.current(),
                    meteredAtCompletion.map(resolution -> tracker.isMeteringCurrentThread())
                )
            );
            assertTrue("merging the mappings must run inside the dispatching query's measurement", safeGet(meteredAtCompletion));
        } finally {
            terminate(threadPool);
        }
    }
}
