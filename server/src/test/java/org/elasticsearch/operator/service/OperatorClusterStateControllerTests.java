/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.OperatorErrorMetadata;
import org.elasticsearch.cluster.metadata.OperatorMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.operator.action.OperatorClusterUpdateSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperatorClusterStateControllerTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        OperatorClusterStateController controller = new OperatorClusterStateController(clusterService);
        controller.initHandlers(List.of(new OperatorClusterUpdateSettingsAction(clusterSettings)));

        String testJSON = """
            {
                 "metadata": {
                     "version": "1234",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {
                         "indices.recovery.max_bytes_per_sec": "50mb"

                 }
            }
            """;

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            assertEquals(
                "Error processing state change request for operator",
                expectThrows(IllegalStateException.class, () -> controller.process("operator", parser)).getMessage()
            );
        }

        testJSON = """
            {
                 "metadata": {
                     "version": "1234",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {
                         "indices.recovery.max_bytes_per_sec": "50mb",
                         "cluster": {
                             "remote": {
                                 "cluster_one": {
                                     "seeds": [
                                         "127.0.0.1:9300"
                                     ]
                                 }
                             }
                         }
                     }
                 }
            }
            """;

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser);
        }
    }

    public void testUpdateStateTasks() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        RerouteService rerouteService = mock(RerouteService.class);

        when(clusterService.getRerouteService()).thenReturn(rerouteService);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        OperatorUpdateStateTask.OperatorUpdateStateTaskExecutor taskExecutor = new OperatorUpdateStateTask.OperatorUpdateStateTaskExecutor(
            "test",
            state,
            clusterService.getRerouteService()
        );

        AtomicBoolean successCalled = new AtomicBoolean(false);

        OperatorUpdateStateTask task = new OperatorUpdateStateTask(new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty empty) {}

            @Override
            public void onFailure(Exception e) {}
        });

        ClusterStateTaskExecutor.TaskContext<OperatorUpdateStateTask> taskContext = new ClusterStateTaskExecutor.TaskContext<>() {
            @Override
            public OperatorUpdateStateTask getTask() {
                return task;
            }

            @Override
            public void success(Runnable onPublicationSuccess) {
                onPublicationSuccess.run();
                successCalled.set(true);
            }

            @Override
            public void success(Consumer<ClusterState> publishedStateConsumer) {}

            @Override
            public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

            @Override
            public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener) {}

            @Override
            public void onFailure(Exception failure) {}
        };

        ClusterState newState = taskExecutor.execute(state, List.of(taskContext));
        assertEquals(state, newState);
        assertTrue(successCalled.get());

        taskExecutor.clusterStatePublished(state);
        verify(rerouteService, times(1)).reroute(anyString(), any(), any());
    }

    public void testErrorStateTask() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor executor = new OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor(
            "test",
            1L,
            OperatorErrorMetadata.ErrorKind.PARSING,
            List.of("some parse error", "some io error")
        );

        ClusterState newState = executor.execute(state, Collections.emptyList());

        OperatorMetadata operatorMetadata = newState.metadata().operatorState("test");
        assertNotNull(operatorMetadata);
        assertNotNull(operatorMetadata.errorMetadata());
        assertEquals(1L, (long) operatorMetadata.errorMetadata().version());
        assertEquals(OperatorErrorMetadata.ErrorKind.PARSING, operatorMetadata.errorMetadata().errorKind());
        assertThat(operatorMetadata.errorMetadata().errors(), contains("some parse error", "some io error"));
    }

    public void testcheckMetadataVersion() {
        OperatorMetadata operatorMetadata = OperatorMetadata.builder("test").version(123L).build();

        assertTrue(
            OperatorClusterStateController.checkMetadataVersion(operatorMetadata, new OperatorStateVersionMetadata(124L, Version.CURRENT))
        );

        assertFalse(
            OperatorClusterStateController.checkMetadataVersion(operatorMetadata, new OperatorStateVersionMetadata(123L, Version.CURRENT))
        );

        assertFalse(
            OperatorClusterStateController.checkMetadataVersion(
                operatorMetadata,
                new OperatorStateVersionMetadata(124L, Version.fromId(Version.CURRENT.id + 1))
            )
        );
    }
}
