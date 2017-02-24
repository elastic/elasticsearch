/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuditorTests extends ESTestCase {
    private Client client;
    private ClusterService clusterService;
    private ListenableActionFuture<IndexResponse> indexResponse;
    private ArgumentCaptor<String> indexCaptor;
    private ArgumentCaptor<String> typeCaptor;
    private ArgumentCaptor<XContentBuilder> jsonCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        DiscoveryNode dNode = mock(DiscoveryNode.class);
        when(dNode.getName()).thenReturn("this_node_has_a_name");
        when(clusterService.localNode()).thenReturn(dNode);

        indexResponse = mock(ListenableActionFuture.class);
        indexCaptor = ArgumentCaptor.forClass(String.class);
        typeCaptor = ArgumentCaptor.forClass(String.class);
        jsonCaptor = ArgumentCaptor.forClass(XContentBuilder.class);
    }

    public void testInfo() throws IOException {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, clusterService);
        auditor.info("foo", "Here is my info");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("foo", auditMessage.getJobId());
        assertEquals("Here is my info", auditMessage.getMessage());
        assertEquals(Level.INFO, auditMessage.getLevel());
    }

    public void testWarning() throws IOException {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, clusterService);
        auditor.warning("bar", "Here is my warning");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("bar", auditMessage.getJobId());
        assertEquals("Here is my warning", auditMessage.getMessage());
        assertEquals(Level.WARNING, auditMessage.getLevel());
    }

    public void testError() throws IOException {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, clusterService);
        auditor.error("foobar", "Here is my error");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("foobar", auditMessage.getJobId());
        assertEquals("Here is my error", auditMessage.getMessage());
        assertEquals(Level.ERROR, auditMessage.getLevel());
    }

    private void givenClientPersistsSuccessfully() {
        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilder.setSource(jsonCaptor.capture())).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.execute()).thenReturn(indexResponse);
        when(client.prepareIndex(indexCaptor.capture(), typeCaptor.capture(), any()))
        .thenReturn(indexRequestBuilder);
        when(client.prepareIndex(indexCaptor.capture(), typeCaptor.capture(), any()))
        .thenReturn(indexRequestBuilder);
    }

    private AuditMessage parseAuditMessage() throws IOException {
        String json = jsonCaptor.getValue().string();
        XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        return AuditMessage.PARSER.apply(parser, null);
    }
}
