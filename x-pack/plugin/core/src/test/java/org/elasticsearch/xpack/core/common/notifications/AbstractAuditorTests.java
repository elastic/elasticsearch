/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractAuditorTests extends ESTestCase {

    private static final String TEST_NODE_NAME = "node_1";
    private static final String TEST_ORIGIN = "test_origin";
    private static final String TEST_INDEX = "test_index";

    private static final int TEST_TEMPLATE_VERSION = 23456789;

    private Client client;
    private ArgumentCaptor<IndexRequest> indexRequestCaptor;
    private long startMillis;

    private ThreadPool threadPool;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        ThreadPool mockPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockPool);
        when(mockPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        startMillis = System.currentTimeMillis();

        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void shutdownThreadPool() {
        threadPool.generic().shutdownNow();
        terminate(threadPool);
    }

    public void testInfo() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.info("foo", "Here is my info");

        verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("foo"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my info"));
        assertThat(auditMessage.getLevel(), equalTo(Level.INFO));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testWarning() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.warning("bar", "Here is my warning");

        verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("bar"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my warning"));
        assertThat(auditMessage.getLevel(), equalTo(Level.WARNING));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testError() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.error("foobar", "Here is my error");

        verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("foobar"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my error"));
        assertThat(auditMessage.getLevel(), equalTo(Level.ERROR));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testAudit() throws IOException {
        Level level = randomFrom(Level.ERROR, Level.INFO, Level.WARNING);

        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.audit(level, "r_id", "Here is my audit");

        verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo("r_id"));
        assertThat(auditMessage.getMessage(), equalTo("Here is my audit"));
        assertThat(auditMessage.getLevel(), equalTo(level));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testAuditingBeforeTemplateInstalled() throws Exception {
        CountDownLatch writeSomeDocsBeforeTemplateLatch = new CountDownLatch(1);
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithoutTemplate(
            writeSomeDocsBeforeTemplateLatch
        );

        auditor.error("foobar", "Here is my error to queue");
        auditor.warning("foobar", "Here is my warning to queue");
        auditor.info("foobar", "Here is my info to queue");

        verify(client, never()).execute(eq(TransportIndexAction.TYPE), any(), any());
        // fire the put template response
        writeSomeDocsBeforeTemplateLatch.countDown();

        // the back log will be written some point later
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        assertBusy(() -> verify(client, times(1)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any()));

        BulkRequest bulkRequest = bulkCaptor.getValue();
        assertThat(bulkRequest.numberOfActions(), equalTo(3));

        auditor.info("foobar", "Here is another message");
        verify(client, times(1)).execute(eq(TransportIndexAction.TYPE), any(), any());
    }

    public void testMaxBufferSize() throws Exception {
        CountDownLatch writeSomeDocsBeforeTemplateLatch = new CountDownLatch(1);
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithoutTemplate(
            writeSomeDocsBeforeTemplateLatch
        );

        int numThreads = 2;
        int numMessagesToWrite = (AbstractAuditor.MAX_BUFFER_SIZE / numThreads) + 10;
        Runnable messageWrites = () -> {
            for (int i = 0; i < numMessagesToWrite; i++) {
                auditor.info("foobar", "filling the buffer");
            }
        };

        Future<?> future1 = threadPool.generic().submit(messageWrites);
        Future<?> future2 = threadPool.generic().submit(messageWrites);
        future1.get();
        future2.get();

        assertThat(auditor.backLogSize(), equalTo(AbstractAuditor.MAX_BUFFER_SIZE));
    }

    private static AbstractAuditMessageTests.TestAuditMessage parseAuditMessage(BytesReference msg) throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, msg.streamInput());
        return AbstractAuditMessageTests.TestAuditMessage.PARSER.apply(parser, null);
    }

    private TestAuditor createTestAuditorWithTemplateInstalled() {
        Map<String, IndexTemplateMetadata> templates = Map.of(TEST_INDEX, mock(IndexTemplateMetadata.class));
        Map<String, ComposableIndexTemplate> templatesV2 = Collections.singletonMap(TEST_INDEX, mock(ComposableIndexTemplate.class));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getTemplates()).thenReturn(templates);
        when(metadata.templatesV2()).thenReturn(templatesV2);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new TestAuditor(client, TEST_NODE_NAME, clusterService);
    }

    @SuppressWarnings("unchecked")
    private TestAuditor createTestAuditorWithoutTemplate(CountDownLatch latch) {
        if (Mockito.mockingDetails(client).isMock() == false) {
            throw new AssertionError("client should be a mock");
        }

        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];

            Runnable onPutTemplate = () -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(e);
                    return;
                }
                listener.onResponse(AcknowledgedResponse.TRUE);
            };

            threadPool.generic().submit(onPutTemplate);

            return null;
        }).when(client).execute(eq(TransportPutComposableIndexTemplateAction.TYPE), any(), any());

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getTemplates()).thenReturn(Map.of());
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new TestAuditor(client, TEST_NODE_NAME, clusterService);
    }

    public static class TestAuditor extends AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> {

        TestAuditor(Client client, String nodeName, ClusterService clusterService) {
            super(
                new OriginSettingClient(client, TEST_ORIGIN),
                TEST_INDEX,
                new IndexTemplateConfig(
                    TEST_INDEX,
                    "/ml/notifications_index_template.json",
                    TEST_TEMPLATE_VERSION,
                    "xpack.ml.version",
                    Map.of(
                        "xpack.ml.version.id",
                        String.valueOf(TEST_TEMPLATE_VERSION),
                        "xpack.ml.notifications.mappings",
                        NotificationsIndex.mapping()
                    )
                ),
                nodeName,
                AbstractAuditMessageTests.TestAuditMessage::new,
                clusterService
            );
        }
    }
}
