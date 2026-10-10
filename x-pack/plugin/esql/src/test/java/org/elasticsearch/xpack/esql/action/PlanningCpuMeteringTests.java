/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks that planning-time resolvers whose responses complete on another thread carry the dispatching query's
 * {@link PlanningCpuTracker} metering over to that thread. Each resolver gets a client that completes on a pool
 * thread, as in production, and the test asserts the listener runs inside the dispatching query's measurement.
 * Resolvers that own a dedicated test class for this (external sources, datasets, globs, the listing cache) are
 * covered there; this class is for those whose metering is a single hand-off.
 */
public class PlanningCpuMeteringTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        assumeTrue("thread CPU time unsupported", ThreadCpuTimer.currentNanos() >= 0);
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    /**
     * The field caps response completes on another thread, so the resolver must carry the dispatching query's planning
     * CPU metering over to it: merging the mappings, and the listener it completes inline, run inside that query's
     * measurement.
     */
    public void testIndexResolverResponseIsMetered() {
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
    }

    /**
     * The view response is forked to the search pool, so the resolver must carry the dispatching query's planning CPU
     * metering over to it: the response handling, and the listener it completes inline, run inside that query's
     * measurement.
     */
    public void testViewResolverResponseIsMetered() {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(ViewResolver.MAX_VIEW_DEPTH_SETTING);
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(
                threadPool,
                new ClusterSettings(Settings.EMPTY, settings)
            )
        ) {
            NoOpClient client = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertSame(EsqlResolveViewAction.TYPE, action);
                    threadPool.generic()
                        .execute(() -> listener.onResponse((Response) new EsqlResolveViewAction.Response(new View[0], null)));
                }
            };
            TestViewResolver resolver = new TestViewResolver(threadPool, clusterService, client);
            PlanningCpuTracker tracker = new PlanningCpuTracker();
            PlainActionFuture<Boolean> meteredAtCompletion = new PlainActionFuture<>();
            tracker.meteredCpu(
                () -> resolver.resolveViews(
                    new EsqlResolveViewAction.Request(TEST_REQUEST_TIMEOUT, false),
                    meteredAtCompletion.map(response -> tracker.isMeteringCurrentThread())
                )
            );
            assertTrue("the view response must be handled inside the dispatching query's measurement", safeGet(meteredAtCompletion));
        }
    }

    /** Exposes the protected request dispatch to this package. */
    private static class TestViewResolver extends ViewResolver {
        TestViewResolver(ThreadPool threadPool, ClusterService clusterService, NoOpClient client) {
            super(threadPool, clusterService, DefaultProjectResolver.INSTANCE, client, CrossProjectModeDecider.NOOP);
        }

        void resolveViews(EsqlResolveViewAction.Request request, ActionListener<EsqlResolveViewAction.Response> listener) {
            doEsqlResolveViewsRequest(request, listener);
        }
    }
}
