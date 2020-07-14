/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecatedMessage;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.deprecation.DeprecationIndexingService.WRITE_DEPRECATION_LOGS_TO_INDEX;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DeprecationIndexingServiceTests extends ESTestCase {

    private DeprecationIndexingService service;
    private ClusterService clusterService;
    private Consumer<IndexRequest> consumer;

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() {
        consumer = mock(Consumer.class);
        clusterService = mock(ClusterService.class);
        service = new DeprecationIndexingService(clusterService, consumer);
    }

    /**
     * Checks that the service registers a cluster state listener, so that the service
     * can be enabled and disabled.
     */
    public void testClusterStateListenerRegistered() {
        verify(clusterService).addListener(service);
    }

    /**
     * Checks that the service does not attempt to index messages when it had not reach the
     * "started" lifecycle state.
     */
    public void testDoesNotWriteMessageWhenServiceNotStarted() {
        service.log("a key", DeprecatedMessage.of(null, "a message"));

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that the service does not attempt to index messages when the service
     * is disabled.
     */
    public void testDoesNotWriteMessageWhenServiceDisabled() {
        service.start();

        service.log("a key", DeprecatedMessage.of(null, "a message"));

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that the service can be disabled after being enabled.
     */
    public void testDoesNotWriteMessageWhenServiceEnabledAndDisabled() {
        service.start();
        service.clusterChanged(getEvent(true));
        service.clusterChanged(getEvent(false));

        service.log("a key", DeprecatedMessage.of(null, "a message"));

        verify(consumer, never()).accept(any());
    }

    /**
     * Checks that messages are indexed in the correct shape when the service is enabled.
     */
    public void testWritesMessageWhenServiceEnabled() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", DeprecatedMessage.of(null, "a message"));

        assertThat(payloadMap, hasKey("@timestamp"));
        assertThat(payloadMap, hasEntry("key", "a key"));
        assertThat(payloadMap, hasEntry("message", "a message"));
        assertThat(payloadMap, hasEntry("cluster.uuid", "cluster-uuid"));
        assertThat(payloadMap, hasEntry("node.id", "local-node-id"));
        // Neither of these should exist since we passed null when writing the message
        assertThat(payloadMap, not(hasKey("x-opaque-id")));
    }

    /**
     * Check that if an xOpaqueId is set, then it is added to the index request payload.
     */
    public void testMessageIncludesOpaqueIdWhenSupplied() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", DeprecatedMessage.of("an ID", "a message"));

        assertThat(payloadMap, hasEntry("x-opaque-id", "an ID"));
    }

    /**
     * Check that if any arguments are set, then they substituted in the log message
     */
    public void testMessageSubstitutesArgumentsWhenSupplied() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest(
            "a key",
            DeprecatedMessage.of(null, "a {} and {} message", "first", "second")
        );

        assertThat(payloadMap, hasEntry("message", "a first and second message"));
    }

    private ClusterChangedEvent getEvent(boolean shouldWriteDeprecationLogs) {
        Settings settings = Settings.builder().put(WRITE_DEPRECATION_LOGS_TO_INDEX.getKey(), shouldWriteDeprecationLogs).build();
        final Metadata metadata = Metadata.builder().clusterUUID("cluster-uuid").transientSettings(settings).build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("local-node-id").build();

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).nodes(nodes).build();

        return new ClusterChangedEvent("test", clusterState, clusterState);
    }

    /*
     * Wraps up the steps for extracting an index request payload from the mocks.
     */
    private Map<String, Object> getWriteRequest(String key, ESLogMessage message) {
        service.log(key, message);

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(consumer).accept(argument.capture());

        final IndexRequest indexRequest = argument.getValue();
        return indexRequest.sourceAsMap();
    }
}
