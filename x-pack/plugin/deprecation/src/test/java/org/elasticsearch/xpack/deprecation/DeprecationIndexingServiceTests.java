/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.deprecation.DeprecationIndexingService.WRITE_DEPRECATION_LOGS_TO_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DeprecationIndexingServiceTests extends ESTestCase {

    private DeprecationIndexingService service;
    private ClusterService clusterService;
    private Client client;

    @Before
    public void initialize() {
        clusterService = mock(ClusterService.class);
        client = spy(new NoOpClient(this.getTestName()));
        service = new DeprecationIndexingService(clusterService, client);
    }

    @After
    public void cleanup() {
        client.close();
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
        service.log("a key", "xOpaqueId", new ESLogMessage("a message"));

        verify(client, never()).execute(any(), any(), anyListener());
    }

    /**
     * Checks that the service does not attempt to index messages when the service
     * is disabled.
     */
    public void testDoesNotWriteMessageWhenServiceDisabled() {
        service.start();

        service.log("a key", "xOpaqueId", new ESLogMessage("a message"));

        verify(client, never()).execute(any(), any(), anyListener());
    }

    /**
     * Checks that the service can be disabled after being enabled.
     */
    public void testDoesNotWriteMessageWhenServiceEnabledAndDisabled() {
        service.start();
        service.clusterChanged(getEvent(true));
        service.clusterChanged(getEvent(false));

        service.log("a key", "xOpaqueId", new ESLogMessage("a message"));

        verify(client, never()).execute(eq(IndexAction.INSTANCE), any(), anyListener());
    }

    /**
     * Checks that messages are indexed in the correct shape when the service is enabled.
     */
    public void testWritesMessageWhenServiceEnabled() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", null, new ESLogMessage("a message"));

        assertThat(payloadMap, hasKey("@timestamp"));
        assertThat(payloadMap, hasEntry("key", "a key"));
        assertThat(payloadMap, hasEntry("message", "a message"));
        // Neither of these should exist since we passed null when writing the message
        assertThat(payloadMap, not(hasKey("x-opaque-id")));
        assertThat(payloadMap, not(hasKey("params")));
    }

    /**
     * Check that if an xOpaqueId is set, then it is added to the index request payload.
     */
    public void testMessageIncludesOpaqueIdWhenSupplied() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", "an ID", new ESLogMessage("a message"));

        assertThat(payloadMap, hasEntry("x-opaque-id", "an ID"));
    }

    /**
     * Check that if any params are set, then they are added to the index request payload.
     */
    public void testMessageIncludesParamsWhenSupplied() {
        service.start();
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", null, new ESLogMessage("a {} and {} message", "first", "second"));

        // I can't get this to work as a one-liner. Curse you, Hamcrest.
        assertThat(payloadMap, hasKey("params"));
        assertThat(payloadMap.get("params"), equalTo(List.of("first", "second")));
    }

    private ClusterChangedEvent getEvent(boolean enabled) {
        Settings settings = Settings.builder().put(WRITE_DEPRECATION_LOGS_TO_INDEX.getKey(), enabled).build();
        final Metadata metadata = Metadata.builder().transientSettings(settings).build();
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        return new ClusterChangedEvent("test", clusterState, clusterState);
    }

    // This exists to silence a generics warning
    private <T> ActionListener<T> anyListener() {
        return any();
    }

    /*
     * Wraps up the steps for extracting an index request payload from the mocks.
     */
    private Map<String, Object> getWriteRequest(String key, String xOpaqueId, ESLogMessage message) {
        service.log(key, xOpaqueId, message);

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(client).execute(eq(IndexAction.INSTANCE), argument.capture(), anyListener());

        return argument.getValue().sourceAsMap();
    }
}
