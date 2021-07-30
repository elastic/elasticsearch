/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor =
            createTestAuditorWithTemplateInstalled(client, Version.CURRENT);
        auditor.info("foo", "Here is my info");

        verify(client).execute(eq(IndexAction.INSTANCE), indexRequestCaptor.capture(), any());
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
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor =
            createTestAuditorWithTemplateInstalled(client, Version.CURRENT);
        auditor.warning("bar", "Here is my warning");

        verify(client).execute(eq(IndexAction.INSTANCE), indexRequestCaptor.capture(), any());
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
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor =
            createTestAuditorWithTemplateInstalled(client, Version.CURRENT);
        auditor.error("foobar", "Here is my error");

        verify(client).execute(eq(IndexAction.INSTANCE), indexRequestCaptor.capture(), any());
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

    public void testAuditingBeforeTemplateInstalled() throws Exception {
        CountDownLatch writeSomeDocsBeforeTemplateLatch = new CountDownLatch(1);
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor =
            // TODO: Both this call and the called method can be simplified in versions that will never have to talk to 7.13
            createTestAuditorWithoutTemplate(client, randomFrom(Version.CURRENT, Version.V_7_13_0), writeSomeDocsBeforeTemplateLatch);

        auditor.error("foobar", "Here is my error to queue");
        auditor.warning("foobar", "Here is my warning to queue");
        auditor.info("foobar", "Here is my info to queue");

        verify(client, never()).execute(eq(IndexAction.INSTANCE), any(), any());
        // fire the put template response
        writeSomeDocsBeforeTemplateLatch.countDown();

        // the back log will be written some point later
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        assertBusy(() ->
            verify(client, times(1)).execute(eq(BulkAction.INSTANCE), bulkCaptor.capture(), any())
        );

        BulkRequest bulkRequest = bulkCaptor.getValue();
        assertThat(bulkRequest.numberOfActions(), equalTo(3));

        auditor.info("foobar", "Here is another message");
        verify(client, times(1)).execute(eq(IndexAction.INSTANCE), any(), any());
    }

    public void testMaxBufferSize() throws Exception {
        CountDownLatch writeSomeDocsBeforeTemplateLatch = new CountDownLatch(1);
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor =
            createTestAuditorWithoutTemplate(client, Version.CURRENT, writeSomeDocsBeforeTemplateLatch);

        int numThreads = 2;
        int numMessagesToWrite = (AbstractAuditor.MAX_BUFFER_SIZE / numThreads) + 10;
        Runnable messageWrites = () -> {
            for (int i=0; i<numMessagesToWrite; i++ ) {
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

    private TestAuditor createTestAuditorWithTemplateInstalled(Client client, Version minNodeVersion) {
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates = ImmutableOpenMap.builder(1);
        templates.put(TEST_INDEX, mock(IndexTemplateMetadata.class));
        Map<String, ComposableIndexTemplate> templatesV2 = Collections.singletonMap(TEST_INDEX, mock(ComposableIndexTemplate.class));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getTemplates()).thenReturn(templates.build());
        when(metadata.templatesV2()).thenReturn(templatesV2);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(minNodeVersion);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        when(state.nodes()).thenReturn(nodes);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new TestAuditor(client, TEST_NODE_NAME, clusterService);
    }

    @SuppressWarnings("unchecked")
    private TestAuditor createTestAuditorWithoutTemplate(Client client, Version minNodeVersion, CountDownLatch latch) {
        if (Mockito.mockingDetails(client).isMock() == false) {
            throw new AssertionError("client should be a mock");
        }

        ActionType<AcknowledgedResponse> expectedTemplateAction =
            minNodeVersion.before(Version.CURRENT)
                ? PutIndexTemplateAction.INSTANCE
                : PutComposableIndexTemplateAction.INSTANCE;

        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener =
                (ActionListener<AcknowledgedResponse>)invocationOnMock.getArguments()[2];

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
        }).when(client).execute(eq(expectedTemplateAction), any(), any());

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);

        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates = ImmutableOpenMap.builder(0);
        Metadata metadata = mock(Metadata.class);
        when(metadata.getTemplates()).thenReturn(templates.build());
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(minNodeVersion);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        when(state.nodes()).thenReturn(nodes);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new TestAuditor(client, TEST_NODE_NAME, clusterService);
    }

    public static class TestAuditor extends AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> {

        TestAuditor(Client client, String nodeName, ClusterService clusterService) {
            super(new OriginSettingClient(client, TEST_ORIGIN), TEST_INDEX, Version.CURRENT,
                new IndexTemplateConfig(TEST_INDEX,
                    "/org/elasticsearch/xpack/core/ml/notifications_index_legacy_template.json", Version.CURRENT.id, "xpack.ml.version",
                    Map.of("xpack.ml.version.id", String.valueOf(Version.CURRENT.id),
                        "xpack.ml.notifications.mappings", NotificationsIndex.mapping())),
                new IndexTemplateConfig(TEST_INDEX,
                    "/org/elasticsearch/xpack/core/ml/notifications_index_template.json", Version.CURRENT.id, "xpack.ml.version",
                    Map.of("xpack.ml.version.id", String.valueOf(Version.CURRENT.id),
                        "xpack.ml.notifications.mappings", NotificationsIndex.mapping())),
                nodeName, AbstractAuditMessageTests.TestAuditMessage::new, clusterService);
        }
    }
}
