/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.ReservedAutoscalingPolicyAction;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests that file settings service can properly add autoscaling policies and detect REST clashes
 * with the reserved policies.
 */
public class AutoscalingFileSettingsIT extends AutoscalingIntegTestCase {

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "autoscaling": {
                       "my_autoscaling_policy": {
                         "roles" : [ "data_hot" ],
                         "deciders": {
                           "fixed": {
                           }
                         }
                       },
                       "my_autoscaling_policy_1": {
                         "roles" : [ "data_warm" ],
                         "deciders": {
                           "fixed": {
                           }
                         }
                       }
                 }
             }
        }""";

    private static String testErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "autoscaling": {
                   "my_autoscaling_policy_bad": {
                     "roles" : [ "data_warm" ],
                     "deciders": {
                       "undecided": {
                       }
                     }
                   }
                 }
             }
        }""";

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        assertTrue(fileSettingsService.watching());

        Files.deleteIfExists(fileSettingsService.operatorSettingsFile());

        Files.createDirectories(fileSettingsService.operatorSettingsDir());
        Path tempFilePath = createTempFile();

        logger.info("--> writing JSON config to node {} with path {}", node, tempFilePath);
        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.operatorSettingsFile(), StandardCopyOption.ATOMIC_MOVE);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedAutoscalingPolicyAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("my_autoscaling_policy")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPoliciesSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get()))
            .actionGet();

        ReservedStateMetadata reservedState = clusterStateResponse.getState()
            .metadata()
            .reservedStateMetadata()
            .get(FileSettingsService.NAMESPACE);

        ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedAutoscalingPolicyAction.NAME);

        assertThat(handlerMetadata.keys(), allOf(notNullValue(), containsInAnyOrder("my_autoscaling_policy", "my_autoscaling_policy_1")));

        // Try using the REST API to update the my_autoscaling_policy policy
        // This should fail, we have reserved certain autoscaling policies in operator mode
        assertEquals(
            "Failed to process request [org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction$Request/unset] "
                + "with errors: [[my_autoscaling_policy] set as read-only by [file_settings]]",
            expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(PutAutoscalingPolicyAction.INSTANCE, sampleRestRequest("my_autoscaling_policy")).actionGet()
            ).getMessage()
        );
    }

    public void testPoliciesApplied() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName());
        writeJSONFile(internalCluster().getMasterName(), testJSON);

        assertPoliciesSaveOK(savedClusterState.v1(), savedClusterState.v2());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.VALIDATION, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("java.lang.IllegalArgumentException: unknown decider [undecided]")
                    );
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertPoliciesNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        // This should succeed, nothing was reserved
        client().execute(PutAutoscalingPolicyAction.INSTANCE, sampleRestRequest("my_autoscaling_policy_bad")).actionGet();
    }

    public void testErrorSaved() throws Exception {
        ensureGreen();
        var savedClusterState = setupClusterStateListenerForError(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), testErrorJSON);
        assertPoliciesNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    private PutAutoscalingPolicyAction.Request sampleRestRequest(String name) throws Exception {
        var json = """
            {
                "roles" : [ "data_cold" ],
                "deciders": {
                  "fixed": {
                  }
                }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return PutAutoscalingPolicyAction.Request.parse(parser, name);
        }
    }
}
