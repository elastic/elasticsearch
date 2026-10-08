/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.HashSet;
import java.util.Set;

public class ViewResolverTests extends ESTestCase {

    /**
     * The view response is forked to the search pool, so the resolver must carry the dispatching query's planning CPU
     * metering over to it: the response handling, and the listener it completes inline, run inside that query's
     * measurement.
     */
    public void testResponseIsMeteredAsPlanningCpuOfTheDispatchingQuery() {
        assumeTrue("thread CPU time unsupported", ThreadCpuTimer.currentNanos() >= 0);
        ThreadPool threadPool = new TestThreadPool(getTestName());
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
            ViewResolver resolver = new ViewResolver(
                threadPool,
                clusterService,
                DefaultProjectResolver.INSTANCE,
                client,
                CrossProjectModeDecider.NOOP
            );
            PlanningCpuTracker tracker = new PlanningCpuTracker();
            PlainActionFuture<Boolean> meteredAtCompletion = new PlainActionFuture<>();
            tracker.meteredCpu(
                () -> resolver.doEsqlResolveViewsRequest(
                    new EsqlResolveViewAction.Request(TEST_REQUEST_TIMEOUT, false),
                    meteredAtCompletion.map(response -> tracker.isMeteringCurrentThread())
                )
            );
            assertTrue("the view response must be handled inside the dispatching query's measurement", safeGet(meteredAtCompletion));
        } finally {
            terminate(threadPool);
        }
    }
}
