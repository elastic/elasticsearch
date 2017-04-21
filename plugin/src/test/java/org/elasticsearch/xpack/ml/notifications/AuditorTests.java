/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuditorTests extends ESTestCase {
    private Client client;
    private ClusterService clusterService;
    private ArgumentCaptor<IndexRequest> indexRequestCaptor;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        DiscoveryNode dNode = mock(DiscoveryNode.class);
        when(dNode.getName()).thenReturn("this_node_has_a_name");
        when(clusterService.localNode()).thenReturn(dNode);

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
    }

    public void testInfo() throws IOException {
        Auditor auditor = new Auditor(client, clusterService);
        auditor.info("foo", "Here is my info");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {".ml-notifications"}, indexRequest.indices());
        assertEquals("audit_message", indexRequest.type());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("foo", auditMessage.getJobId());
        assertEquals("Here is my info", auditMessage.getMessage());
        assertEquals(Level.INFO, auditMessage.getLevel());
    }

    public void testWarning() throws IOException {
        Auditor auditor = new Auditor(client, clusterService);
        auditor.warning("bar", "Here is my warning");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {".ml-notifications"}, indexRequest.indices());
        assertEquals("audit_message", indexRequest.type());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("bar", auditMessage.getJobId());
        assertEquals("Here is my warning", auditMessage.getMessage());
        assertEquals(Level.WARNING, auditMessage.getLevel());
    }

    public void testError() throws IOException {
        Auditor auditor = new Auditor(client, clusterService);
        auditor.error("foobar", "Here is my error");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {".ml-notifications"}, indexRequest.indices());
        assertEquals("audit_message", indexRequest.type());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("foobar", auditMessage.getJobId());
        assertEquals("Here is my error", auditMessage.getMessage());
        assertEquals(Level.ERROR, auditMessage.getLevel());
    }

    private AuditMessage parseAuditMessage(BytesReference msg) throws IOException {
        XContentParser parser = XContentFactory.xContent(msg).createParser(NamedXContentRegistry.EMPTY, msg);
        return AuditMessage.PARSER.apply(parser, null);
    }
}
