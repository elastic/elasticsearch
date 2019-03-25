/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuditorTests extends ESTestCase {
    private Client client;
    private ArgumentCaptor<IndexRequest> indexRequestCaptor;
    private static final String TEST_ORIGIN = "test_origin";
    private static final String TEST_INDEX = "test_index";
    private static final AbstractAuditMessage.AbstractBuilder<AbstractAuditMessageTests.TestAuditMessage> builder = 
        AbstractAuditMessageTests.TestAuditMessage.newBuilder();
    
    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
    }

    public void testInfo() throws IOException {
        Auditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new Auditor<>(client, "node_1", TEST_INDEX, TEST_ORIGIN, builder);
        auditor.info("foo", "Here is my info");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {TEST_INDEX}, indexRequest.indices());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("foo", auditMessage.getResourceId());
        assertEquals("Here is my info", auditMessage.getMessage());
        assertEquals(Level.INFO, auditMessage.getLevel());
    }

    public void testWarning() throws IOException {
        Auditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new Auditor<>(client, "node_1", TEST_INDEX, TEST_ORIGIN, builder);
        auditor.warning("bar", "Here is my warning");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {TEST_INDEX}, indexRequest.indices());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("bar", auditMessage.getResourceId());
        assertEquals("Here is my warning", auditMessage.getMessage());
        assertEquals(Level.WARNING, auditMessage.getLevel());
    }

    public void testError() throws IOException {
        Auditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new Auditor<>(client, "node_1", TEST_INDEX, TEST_ORIGIN, builder);
        auditor.error("foobar", "Here is my error");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertArrayEquals(new String[] {TEST_INDEX}, indexRequest.indices());
        assertEquals(TimeValue.timeValueSeconds(5), indexRequest.timeout());
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertEquals("foobar", auditMessage.getResourceId());
        assertEquals("Here is my error", auditMessage.getMessage());
        assertEquals(Level.ERROR, auditMessage.getLevel());
    }

    private AbstractAuditMessageTests.TestAuditMessage parseAuditMessage(BytesReference msg) throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentHelper.xContentType(msg))
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, msg.streamInput());
        return AbstractAuditMessageTests.TestAuditMessage.PARSER.apply(parser, null);
    }
}
