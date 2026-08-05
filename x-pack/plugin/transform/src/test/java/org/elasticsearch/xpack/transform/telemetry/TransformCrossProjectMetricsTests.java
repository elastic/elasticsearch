/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.telemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.transforms.TransformContext;
import org.elasticsearch.xpack.transform.transforms.TransformTask;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TransformCrossProjectMetrics}. Mocked tasks are registered in a real
 * {@link TransformNode} registry, as the persistent-task executor does in production.
 * {@link TransformCrossProjectMetrics#poll()} is called directly to refresh the cached counts
 * (the background scheduler is never started). The CPS-enabled/transform-node gate lives in
 * {@code Transform.createComponents} — on other nodes the component is never constructed.
 */
public class TransformCrossProjectMetricsTests extends ESTestCase {

    private final TransformNode transformNode = new TransformNode(Optional::empty);
    private final Map<String, Supplier<Collection<LongWithAttributes>>> observersByMetricName = new HashMap<>();

    private TransformCrossProjectMetrics component;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        var meterRegistry = mock(MeterRegistry.class);
        when(meterRegistry.registerLongsGauge(anyString(), anyString(), anyString(), any())).thenAnswer(inv -> {
            observersByMetricName.put(inv.getArgument(0), inv.getArgument(3));
            return mock(LongGauge.class);
        });

        component = new TransformCrossProjectMetrics(
            meterRegistry,
            mock(ThreadPool.class),
            TransformCrossProjectMetrics.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            transformNode
        );
    }

    private Collection<LongWithAttributes> observeUiamAuth() {
        return observersByMetricName.get(TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT).get();
    }

    private Collection<LongWithAttributes> observeActive() {
        return observersByMetricName.get(TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT).get();
    }

    /** Registers a mocked running task with the given per-search facts ({@code null} = no search yet). */
    private void registerTask(String transformId, Boolean uiamAuth, Boolean crossProject) {
        var task = mock(TransformTask.class);
        var context = mock(TransformContext.class);
        when(task.getTransformId()).thenReturn(transformId);
        when(task.getContext()).thenReturn(context);
        when(context.getUiamAuth()).thenReturn(uiamAuth);
        when(context.getLastSearchCrossProject()).thenReturn(crossProject);
        transformNode.registerTransform(task);
    }

    public void testActiveRegistersBothGauges() {
        assertThat(
            observersByMetricName.keySet(),
            containsInAnyOrder(
                TransformCrossProjectMetrics.TRANSFORM_CPS_UIAM_AUTH_CURRENT,
                TransformCrossProjectMetrics.TRANSFORM_CPS_ACTIVE_CURRENT
            )
        );
    }

    public void testNoTasksEmitsNothing() {
        component.poll();
        assertThat(observeUiamAuth(), empty());
        assertThat(observeActive(), empty());
    }

    public void testObserversReadCacheNotLiveTasks() {
        registerTask("transform-1", true, true);

        // no poll yet -> the cached counts are still empty
        assertThat(observeUiamAuth(), empty());
        assertThat(observeActive(), empty());

        component.poll();
        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(0L, Map.of("auth_type", "legacy"))
            )
        );
        assertThat(
            observeActive(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("scope", "cross_project")),
                new LongWithAttributes(0L, Map.of("scope", "origin"))
            )
        );
    }

    public void testTasksWithoutCompletedSearchAreExcluded() {
        registerTask("pending-1", null, null);
        component.poll();
        assertThat("no search recorded yet -> no observation", observeUiamAuth(), empty());
        assertThat("no search recorded yet -> no observation", observeActive(), empty());
    }

    public void testUiamMigratedCount() {
        // Two migrated (UIAM credential), one not migrated (legacy)
        registerTask("uiam-1", true, false);
        registerTask("uiam-2", true, false);
        registerTask("legacy-1", false, false);
        component.poll();

        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(2L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(1L, Map.of("auth_type", "legacy"))
            )
        );
    }

    public void testAllMigrated() {
        registerTask("uiam-1", true, false);
        registerTask("uiam-2", true, false);
        component.poll();

        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(2L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(0L, Map.of("auth_type", "legacy"))
            )
        );
    }

    public void testAllLegacy() {
        registerTask("legacy-1", false, false);
        registerTask("legacy-2", false, false);
        component.poll();

        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(0L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(2L, Map.of("auth_type", "legacy"))
            )
        );
    }

    public void testCrossProjectActiveCount() {
        // One transform's last search went cross-project, two stayed in the origin project
        registerTask("cp-1", true, true);
        registerTask("origin-1", true, false);
        registerTask("origin-2", false, false);
        component.poll();

        assertThat(
            observeActive(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("scope", "cross_project")),
                new LongWithAttributes(2L, Map.of("scope", "origin"))
            )
        );
    }

    public void testAllCrossProject() {
        registerTask("cp-1", true, true);
        registerTask("cp-2", false, true);
        component.poll();

        assertThat(
            observeActive(),
            containsInAnyOrder(
                new LongWithAttributes(2L, Map.of("scope", "cross_project")),
                new LongWithAttributes(0L, Map.of("scope", "origin"))
            )
        );
    }

    public void testBothGaugesObserveTheSameSnapshot() {
        registerTask("transform-1", true, true);
        component.poll();

        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(0L, Map.of("auth_type", "legacy"))
            )
        );
        assertThat(
            observeActive(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("scope", "cross_project")),
                new LongWithAttributes(0L, Map.of("scope", "origin"))
            )
        );
    }

    public void testCachedCountsResetWhenTasksDeregistered() {
        registerTask("transform-1", true, true);
        component.poll();
        assertThat(
            observeUiamAuth(),
            containsInAnyOrder(
                new LongWithAttributes(1L, Map.of("auth_type", "uiam")),
                new LongWithAttributes(0L, Map.of("auth_type", "legacy"))
            )
        );

        // transform stopped -> the task deregistered itself -> next poll resets the cache
        transformNode.deregisterTransform("transform-1");
        component.poll();
        assertThat("node now idle -> no observation", observeUiamAuth(), empty());
        assertThat("node now idle -> no observation", observeActive(), empty());
    }
}
