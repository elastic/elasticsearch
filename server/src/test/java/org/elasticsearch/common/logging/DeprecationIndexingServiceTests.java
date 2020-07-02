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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.MalformedParametersException;

import static org.elasticsearch.common.logging.DeprecationIndexingService.WRITE_DEPRECATION_LOGS_TO_INDEX;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    public void testClusterStateListenerRegistered() {
        verify(clusterService).addListener(service);
    }

    public void testCreatesTemplateOnDemand() {
        service.clusterChanged(getEvent(true));

        verify(client).execute(eq(PutComposableIndexTemplateAction.INSTANCE), any(), anyListener());
    }

    public void testDoesNotWriteMessageWhenServiceDisabled() {
        service.writeMessage("key", "message", "xOpaqueId", null);

        verify(client, never()).execute(any(), any(), anyListener());
    }

    public void testWritesMessageWhenServiceEnabled() {
        service.clusterChanged(getEvent(true));

        service.writeMessage("key", "message", "xOpaqueId", null);

        ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

        verify(client).execute(eq(IndexAction.INSTANCE), argument.capture(), anyListener());

        final IndexRequest request = argument.getValue();

        final String s = request.source().utf8ToString();

        logger
            .warn("");
    }

    public void testWritesMessageInExpectedFormat() {
        service.clusterChanged(getEvent(true));

        service.writeMessage("key", "message", "xOpaqueId", null);

        verify(client).execute(eq(IndexAction.INSTANCE), any(), anyListener());
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

    private class MyNoOpClient extends NoOpClient {
        public MyNoOpClient() {
            super(DeprecationIndexingServiceTests.this.getTestName());
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof PutComposableIndexTemplateAction.Request) {
                listener.onResponse(((Response) new AcknowledgedResponse(true)));
                return;
            }

            super.doExecute(action, request, listener);
        }
    }
}
