/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlDatafeedRestCapabilitiesTests extends ESTestCase {

    public void testDatafeedCapabilitiesReflectDynamicClusterSetting() {
        AtomicReference<ClusterState> state = new AtomicReference<>(clusterStateWithSettings(false, null));
        List<RestHandler> handlers = handlers(true, state);

        for (RestHandler handler : handlers) {
            assertThat(handler.supportedCapabilities(), contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH));
        }

        state.set(clusterStateWithSettings(true, null));
        for (RestHandler handler : handlers) {
            assertThat(
                handler.supportedCapabilities(),
                containsInAnyOrder(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH, MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY)
            );
        }
    }

    public void testDatafeedCapabilitiesProjectSettingsOverrideClusterSetting() {
        AtomicReference<ClusterState> state = new AtomicReference<>(clusterStateWithSettings(false, true));
        List<RestHandler> handlers = handlers(true, state);

        for (RestHandler handler : handlers) {
            assertThat(
                handler.supportedCapabilities(),
                containsInAnyOrder(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH, MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY)
            );
        }

        state.set(clusterStateWithSettings(true, false));
        for (RestHandler handler : handlers) {
            assertThat(handler.supportedCapabilities(), contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH));
        }
    }

    public void testDatafeedCapabilitiesPreserveCrossProjectSearchCapability() {
        AtomicReference<ClusterState> state = new AtomicReference<>(clusterStateWithSettings(false, null));
        for (RestHandler handler : handlers(false, state)) {
            assertThat(handler.supportedCapabilities(), empty());
        }

        state.set(clusterStateWithSettings(true, null));
        for (RestHandler handler : handlers(false, state)) {
            assertThat(handler.supportedCapabilities(), contains(MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY));
        }
    }

    private static List<RestHandler> handlers(boolean mlCrossProjectSearchEnabled, AtomicReference<ClusterState> state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenAnswer(ignored -> state.get());
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectId()).thenReturn(ProjectId.DEFAULT);
        return List.of(
            new RestPutDatafeedAction(mlCrossProjectSearchEnabled, clusterService, projectResolver),
            new RestUpdateDatafeedAction(mlCrossProjectSearchEnabled, clusterService, projectResolver),
            new RestPreviewDatafeedAction(mlCrossProjectSearchEnabled, clusterService, projectResolver)
        );
    }

    private static ClusterState clusterStateWithSettings(Boolean clusterEnabled, Boolean projectEnabled) {
        Settings.Builder clusterSettings = Settings.builder();
        if (clusterEnabled != null) {
            clusterSettings.put(MachineLearning.ESQL_DATAFEEDS_ENABLED.getKey(), clusterEnabled);
        }
        ClusterState.Builder state = ClusterState.builder(new ClusterName("ml-datafeed-rest-capabilities-tests"))
            .metadata(Metadata.builder().persistentSettings(clusterSettings.build()));
        if (projectEnabled != null) {
            Settings projectSettings = Settings.builder().put(MachineLearning.ESQL_DATAFEEDS_ENABLED.getKey(), projectEnabled).build();
            state.putCustom(
                ProjectStateRegistry.TYPE,
                ProjectStateRegistry.builder().putProjectSettings(ProjectId.DEFAULT, projectSettings).build()
            );
        }
        return state.build();
    }
}
