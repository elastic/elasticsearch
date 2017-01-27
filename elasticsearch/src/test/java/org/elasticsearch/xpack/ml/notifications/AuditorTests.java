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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class AuditorTests extends ESTestCase {
    private Client client;
    private ListenableActionFuture<IndexResponse> indexResponse;
    private ArgumentCaptor<String> indexCaptor;
    private ArgumentCaptor<String> typeCaptor;
    private ArgumentCaptor<XContentBuilder> jsonCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        client = Mockito.mock(Client.class);
        indexResponse = Mockito.mock(ListenableActionFuture.class);
        indexCaptor = ArgumentCaptor.forClass(String.class);
        typeCaptor = ArgumentCaptor.forClass(String.class);
        jsonCaptor = ArgumentCaptor.forClass(XContentBuilder.class);
    }

    public void testInfo() {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, "foo");
        auditor.info("Here is my info");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("foo", auditMessage.getJobId());
        assertEquals("Here is my info", auditMessage.getMessage());
        assertEquals(Level.INFO, auditMessage.getLevel());
    }

    public void testWarning() {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, "bar");
        auditor.warning("Here is my warning");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("bar", auditMessage.getJobId());
        assertEquals("Here is my warning", auditMessage.getMessage());
        assertEquals(Level.WARNING, auditMessage.getLevel());
    }

    public void testError() {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, "foobar");
        auditor.error("Here is my error");
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_message", typeCaptor.getValue());
        AuditMessage auditMessage = parseAuditMessage();
        assertEquals("foobar", auditMessage.getJobId());
        assertEquals("Here is my error", auditMessage.getMessage());
        assertEquals(Level.ERROR, auditMessage.getLevel());
    }

    public void testActivity_GivenNumbers() {
        givenClientPersistsSuccessfully();
        Auditor auditor = new Auditor(client, "");
        auditor.activity(10, 100, 5, 50);
        assertEquals(".ml-notifications", indexCaptor.getValue());
        assertEquals("audit_activity", typeCaptor.getValue());
        AuditActivity auditActivity = parseAuditActivity();
        assertEquals(10, auditActivity.getTotalJobs());
        assertEquals(100, auditActivity.getTotalDetectors());
        assertEquals(5, auditActivity.getRunningJobs());
        assertEquals(50, auditActivity.getRunningDetectors());
    }

    private void givenClientPersistsSuccessfully() {
        IndexRequestBuilder indexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
        when(indexRequestBuilder.setSource(jsonCaptor.capture())).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.execute()).thenReturn(indexResponse);
        when(client.prepareIndex(indexCaptor.capture(), typeCaptor.capture(), any()))
        .thenReturn(indexRequestBuilder);
        when(client.prepareIndex(indexCaptor.capture(), typeCaptor.capture(), any()))
        .thenReturn(indexRequestBuilder);
    }

    private AuditMessage parseAuditMessage() {
        try {
            String json = jsonCaptor.getValue().string();
            XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
            return AuditMessage.PARSER.apply(parser, null);
        } catch (IOException e) {
            return new AuditMessage();
        }
    }

    private AuditActivity parseAuditActivity() {
        try {
            String json = jsonCaptor.getValue().string();
            XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
            return AuditActivity.PARSER.apply(parser, null);
        } catch (IOException e) {
            return new AuditActivity();
        }
    }
}
