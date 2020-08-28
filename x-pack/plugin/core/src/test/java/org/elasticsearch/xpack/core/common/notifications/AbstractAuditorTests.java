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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractAuditorTests extends ESTestCase {

    private static final String TEST_NODE_NAME = "node_1";
    private static final String TEST_ORIGIN = "test_origin";
    private static final String TEST_INDEX = "test_index";

    private Client client;
    private ArgumentCaptor<IndexRequest> indexRequestCaptor;
    private long startMillis;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        startMillis = System.currentTimeMillis();
    }

    public void testInfo() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new TestAuditor(client);
        auditor.info("foo", "Here is my info");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("foo"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my info"));
        assertThat(auditMessage.getLevel(), equalTo(Level.INFO));
        assertThat(auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testWarning() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new TestAuditor(client);
        auditor.warning("bar", "Here is my warning");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("bar"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my warning"));
        assertThat(auditMessage.getLevel(), equalTo(Level.WARNING));
        assertThat(auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testError() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = new TestAuditor(client);
        auditor.error("foobar", "Here is my error");

        verify(client).index(indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("foobar"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my error"));
        assertThat(auditMessage.getLevel(), equalTo(Level.ERROR));
        assertThat(auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    private static AbstractAuditMessageTests.TestAuditMessage parseAuditMessage(BytesReference msg) throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentHelper.xContentType(msg))
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, msg.streamInput());
        return AbstractAuditMessageTests.TestAuditMessage.PARSER.apply(parser, null);
    }

    private static class TestAuditor extends AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> {

        TestAuditor(Client client) {
            super(client, TEST_NODE_NAME, TEST_INDEX, TEST_ORIGIN, AbstractAuditMessageTests.TestAuditMessage::new);
        }
    }
}
