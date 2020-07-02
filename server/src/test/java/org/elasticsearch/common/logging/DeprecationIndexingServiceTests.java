/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.DeprecationIndexingService.WRITE_DEPRECATION_LOGS_TO_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DeprecationIndexingServiceTests extends ESTestCase {

    private DeprecationIndexingService service;
    private ClusterService clusterService;
    private Client client;

    @Before
    public void initialize() {
        clusterService = mock(ClusterService.class);
        client = spy(new MyNoOpClient());
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
     * Checks that the service ensures that the data stream template is created when
     * the service is enabled.
     */
    public void testCreatesTemplateOnDemand() {
        service.clusterChanged(getEvent(true));

        verify(client).execute(eq(PutComposableIndexTemplateAction.INSTANCE), any(), anyListener());
    }

    /**
     * Checks that the service only creates the data stream template once,
     * and not every time it processes the cluster state.
     */
    public void testOnlyCreatesTemplateOnce() {
        service.clusterChanged(getEvent(true));
        service.clusterChanged(getEvent(true));

        verify(client, times(1)).execute(eq(PutComposableIndexTemplateAction.INSTANCE), any(), anyListener());
    }

    /**
     * Checks that the service only ensures that the data stream template is created once,
     * even if it fails the first time, in order to prevent a flood of failure messages
     * to the log.
     */
    public void testOnlyCreatesTemplateOnceEvenOnFailure() {
        ((MyNoOpClient) client).setAcknowledged(false);
        service.clusterChanged(getEvent(true));
        service.clusterChanged(getEvent(true));

        verify(client, times(1)).execute(eq(PutComposableIndexTemplateAction.INSTANCE), any(), anyListener());
    }

    /**
     * Checks that the service does not attempt to index messages when the service
     * is disabled.
     */
    public void testDoesNotWriteMessageWhenServiceDisabled() {
        service.writeMessage("key", "message", "xOpaqueId", null);

        verify(client, never()).execute(any(), any(), anyListener());
    }

    /**
     * Checks that the service can be disabled after being enabled.
     */
    public void testDoesNotWriteMessageWhenServiceEnabledAndDisabled() {
        service.clusterChanged(getEvent(true));
        service.clusterChanged(getEvent(false));

        service.writeMessage("key", "message", "xOpaqueId", null);

        verify(client, never()).execute(eq(IndexAction.INSTANCE), any(), anyListener());
    }

    /**
     * Checks that messages are indexed in the correct shape when the service is enabled.
     */
    public void testWritesMessageWhenServiceEnabled() {
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", "a message", null, null);

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
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", "a message", "an ID", null);

        assertThat(payloadMap, hasEntry("x-opaque-id", "an ID"));
    }

    /**
     * Check that if any params are set, then they are added to the index request payload.
     */
    public void testMessageIncludesParamsWhenSupplied() {
        service.clusterChanged(getEvent(true));

        final Map<String, Object> payloadMap = getWriteRequest("a key", "a message", null, new Object[] { "first", "second" });

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
     * A client that does nothing, except for requests of type
     * PutComposableIndexTemplateAction.Request, in which case return an AcknowledgedResponse
     */
    private class MyNoOpClient extends NoOpClient {
        private boolean isAcknowledged = true;

        public MyNoOpClient() {
            super(DeprecationIndexingServiceTests.this.getTestName());
        }

        public void setAcknowledged(boolean acknowledged) {
            isAcknowledged = acknowledged;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof PutComposableIndexTemplateAction.Request) {
                listener.onResponse(((Response) new AcknowledgedResponse(isAcknowledged)));
                return;
            }

            super.doExecute(action, request, listener);
        }

    }

    /*
     * Wraps up the steps for extracting an index request payload from the mocks.
     */
    private Map<String, Object> getWriteRequest(String key, String message, String xOpaqueId, Object[] params) {
        service.writeMessage(key, message, xOpaqueId, params);

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(client).execute(eq(IndexAction.INSTANCE), argument.capture(), anyListener());

        return argument.getValue().sourceAsMap();
    }
}
