/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.SimulateBulkAction;
import org.elasticsearch.action.bulk.SimulateBulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.allocation.ParkedIndexAllocationDecider;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnparkOnWriteFilterTests extends ESTestCase {

    private ThreadPool threadPool;
    private final List<UpdateSettingsRequest> unparks = new ArrayList<>();

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /**
     * Two projects with an index of the same name, parked only in the other project; then parked in the request's project. Returns a
     * filter whose request project is {@code requestProject}.
     */
    private UnparkOnWriteFilter filter(boolean parkedInRequestProject) {
        final ProjectId requestProject = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(ProjectMetadata.builder(requestProject).put(index(parkedInRequestProject), false))
                    .put(ProjectMetadata.builder(otherProject).put(index(true), false))
                    .build()
            )
            .build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.builder().put(UnparkOnWriteFilter.ENABLED_SETTING.getKey(), true).build(),
                Set.of(UnparkOnWriteFilter.ENABLED_SETTING)
            )
        );
        final var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                unparks.add((UpdateSettingsRequest) request);
                listener.onResponse(null);
            }
        };
        return new UnparkOnWriteFilter(() -> clusterService, () -> client, () -> TestProjectResolvers.singleProject(requestProject));
    }

    private static IndexMetadata index(boolean parked) {
        return IndexMetadata.builder("idx")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                    .put(ParkedIndexAllocationDecider.PARKED_SETTING.getKey(), parked)
            )
            .build();
    }

    private void apply(UnparkOnWriteFilter filter, String action, ActionRequest request) {
        final AtomicBoolean proceeded = new AtomicBoolean();
        filter.apply(null, action, request, ActionListener.noop(), (task, a, r, l) -> proceeded.set(true));
        assertTrue("request must always proceed", proceeded.get());
    }

    public void testBulkUnparks() {
        apply(filter(true), TransportBulkAction.NAME, new BulkRequest().add(new IndexRequest("idx").source(Map.of("f", "v"))));
        assertThat(unparks.stream().map(r -> List.of(r.indices())).toList(), contains(List.of("idx")));
    }

    public void testUpdateUnparks() {
        apply(filter(true), TransportUpdateAction.NAME, new UpdateRequest("idx", "1").doc(Map.of("f", "v")));
        assertThat(unparks.stream().map(r -> List.of(r.indices())).toList(), contains(List.of("idx")));
    }

    public void testSimulateBulkDoesNotUnpark() {
        final var simulate = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of(), null);
        simulate.add(new IndexRequest("idx").source(Map.of("f", "v")));
        apply(filter(true), SimulateBulkAction.NAME, simulate);
        assertThat(unparks, empty());
    }

    public void testResolvesIndexInRequestProject() {
        // "idx" is parked in the other project only: a write in the request's project must not unpark anything
        final var filter = filter(false);
        apply(filter, TransportBulkAction.NAME, new BulkRequest().add(new IndexRequest("idx").source(Map.of("f", "v"))));
        apply(filter, TransportUpdateAction.NAME, new UpdateRequest("idx", "1").doc(Map.of("f", "v")));
        assertThat(unparks, empty());
    }
}
