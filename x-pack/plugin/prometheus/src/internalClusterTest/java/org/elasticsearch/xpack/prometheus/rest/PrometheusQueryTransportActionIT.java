/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.prometheus.PrometheusPlugin;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.InstantQuery;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Runs {@link PrometheusQueryTransportAction} on a node, where it runs its ES|QL query as a child of its own task.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class PrometheusQueryTransportActionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // the Prometheus plugin reads x-pack settings, which the x-pack core plugin registers
        plugins.add(LocalStateXPack.class);
        plugins.add(PrometheusPlugin.class);
        plugins.add(HoldingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // the index templates are not needed here, and depend on plugins this cluster doesn't have
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.prometheus.registry.enabled", false)
            .build();
    }

    /**
     * A string literal runs as an ES|QL query too: its response carries the ES|QL response, which outlives the action's own
     * release of it as long as the response is referenced.
     */
    public void testStringLiteral() {
        HoldingPlugin.hold.set(false);
        var request = new PrometheusQueryRequest(new InstantQuery("\"a string\"", "metrics-*", Instant.now()), "\"a string\"");
        PrometheusQueryResponse response = client().execute(PrometheusQueryTransportAction.TYPE, request).actionGet();
        try {
            assertThat(response.resultType(), equalTo("string"));
            assertThat(response.stringValue(), equalTo("a string"));
            assertTrue(response.esqlResponse().hasReferences());
            assertThat(response.esqlResponse().pages(), hasSize(1));
        } finally {
            response.decRef();
        }
    }

    /** Cancelling the action's task, as a client disconnect or a query timeout does, cancels the ES|QL query it runs. */
    public void testCancellingTheTaskCancelsTheEsqlQuery() throws Exception {
        HoldingPlugin.hold.set(true);
        HoldingPlugin.held.set(null);
        var request = new PrometheusQueryRequest(new InstantQuery("\"a string\"", "metrics-*", Instant.now()), "\"a string\"");
        ActionFuture<PrometheusQueryResponse> future = client().execute(PrometheusQueryTransportAction.TYPE, request);
        try {
            assertBusy(() -> assertThat(HoldingPlugin.held.get(), notNullValue()));
            HeldQuery held = HoldingPlugin.held.get();

            List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(PrometheusQueryTransportAction.NAME).get().getTasks();
            assertThat(tasks, hasSize(1));
            TaskId prometheusTask = tasks.getFirst().taskId();
            assertThat("the ES|QL query is a child of the action's task", held.task().getParentTaskId(), equalTo(prometheusTask));

            clusterAdmin().prepareCancelTasks().setTargetTaskId(prometheusTask).get();
            assertBusy(() -> assertTrue("the ES|QL query is cancelled", held.task().isCancelled()));
            held.release().run();

            Exception e = expectThrows(Exception.class, future::actionGet);
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(TaskCancelledException.class));
        } finally {
            HoldingPlugin.hold.set(false);
            HeldQuery held = HoldingPlugin.held.getAndSet(null);
            if (held != null && future.isDone() == false) {
                held.release().run();
            }
        }
    }

    public static class LocalStateXPack extends LocalStateCompositeXPackPlugin {
        public LocalStateXPack(Settings settings, Path configPath) {
            super(settings, configPath);
        }
    }

    private record HeldQuery(CancellableTask task, Runnable release) {}

    /** Holds the ES|QL query of a Prometheus request before it runs, while {@link #hold} is set, until the test releases it. */
    public static class HoldingPlugin extends Plugin implements ActionPlugin {
        static final AtomicBoolean hold = new AtomicBoolean();
        static final AtomicReference<HeldQuery> held = new AtomicReference<>();

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (hold.get() && EsqlQueryAction.NAME.equals(action) && request instanceof PromqlQueryRequest) {
                        held.set(new HeldQuery((CancellableTask) task, () -> chain.proceed(task, action, request, listener)));
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }
    }
}
