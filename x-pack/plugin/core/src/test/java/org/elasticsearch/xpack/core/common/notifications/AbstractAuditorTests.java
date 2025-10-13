/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
    private static final String TEST_INDEX_PREFIX = "test_index";
    private static final String TEST_INDEX_VERSION = "-000001";
    private static final String TEST_INDEX = TEST_INDEX_PREFIX + TEST_INDEX_VERSION;
    private static final String TEST_INDEX_ALIAS = "test_index_write";

    private static final int TEST_TEMPLATE_VERSION = 23456789;

    private Client client;
    private ArgumentCaptor<IndexRequest> indexRequestCaptor;
    private ArgumentCaptor<BulkRequest> bulkRequestCaptor;
    private long startMillis;

    private ThreadPool threadPool;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        ThreadPool mockPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockPool);
        when(mockPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
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
        // The first audit is written as a bulk request from the backlog
        // once the template & alias checks have passed
        verifyBulkIndexed("foo", "Here is my info", Level.INFO);
        // Subsequent messages are indexed directly
        auditor.info("foo", "This message is indexed directly because the write alias exists");
        verifyMessageIndexed("foo", "This message is indexed directly because the write alias exists", Level.INFO);
    }

    public void testWarning() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.warning("bar", "Here is my warning");
        // The first audit is written as a bulk request from the backlog
        // once the template & alias checks have passed
        verifyBulkIndexed("bar", "Here is my warning", Level.WARNING);
        // Subsequent messages are indexed directly
        auditor.warning("bar", "This message is indexed directly because the write alias exists");
        verifyMessageIndexed("bar", "This message is indexed directly because the write alias exists", Level.WARNING);
    }

    public void testError() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.error("foobar", "Here is my error");
        // The first audit is written as a bulk request from the backlog
        // once the template & alias checks have passed
        verifyBulkIndexed("foobar", "Here is my error", Level.ERROR);
        // Subsequent messages are indexed directly
        auditor.error("foobar", "This message is indexed directly because the write alias exists");
        verifyMessageIndexed("foobar", "This message is indexed directly because the write alias exists", Level.ERROR);
    }

    public void testAudit() throws IOException {
        Level level = randomFrom(Level.ERROR, Level.INFO, Level.WARNING);

        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateInstalled();
        auditor.audit(level, "r_id", "Here is my audit");
        // The first audit is written as a bulk request from the backlog
        // once the template & alias checks have passed
        verifyBulkIndexed("r_id", "Here is my audit", level);
        // Subsequent messages are indexed directly
        auditor.audit(level, "r_id", "This message is indexed directly because the write alias exists");
        verifyMessageIndexed("r_id", "This message is indexed directly because the write alias exists", level);
    }

    private void verifyMessageIndexed(String resourceId, String message, Level level) throws IOException {
        verify(client).execute(eq(TransportIndexAction.TYPE), indexRequestCaptor.capture(), any());
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.indices(), arrayContaining(TEST_INDEX_ALIAS));
        assertThat(indexRequest.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo(resourceId));
        assertThat(auditMessage.getMessage(), equalTo(message));
        assertThat(auditMessage.getLevel(), equalTo(level));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    private void verifyBulkIndexed(String resourceId, String message, Level level) throws IOException {
        verify(client).execute(eq(TransportBulkAction.TYPE), bulkRequestCaptor.capture(), any());
        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertThat(bulkRequest.numberOfActions(), is(1));
        assertThat(bulkRequest.timeout(), equalTo(TimeValue.timeValueSeconds(60)));
        var firstBulk = bulkRequest.requests().get(0);
        assertThat(firstBulk.index(), is(TEST_INDEX_ALIAS));
        assertThat(firstBulk, instanceOf(IndexRequest.class));
        var indexRequest = (IndexRequest) firstBulk;
        AbstractAuditMessageTests.TestAuditMessage auditMessage = parseAuditMessage(indexRequest.source());
        assertThat(auditMessage.getResourceId(), equalTo(resourceId));
        assertThat(auditMessage.getMessage(), equalTo(message));
        assertThat(auditMessage.getLevel(), equalTo(level));
        assertThat(
            auditMessage.getTimestamp().getTime(),
            allOf(greaterThanOrEqualTo(startMillis), lessThanOrEqualTo(System.currentTimeMillis()))
        );
        assertThat(auditMessage.getNodeName(), equalTo(TEST_NODE_NAME));
    }

    public void testAuditWithMissingAlias() throws IOException {
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithTemplateAndIndexButNoAlias();
        auditor.info("foobar", "Add the alias first");
        verify(client).execute(eq(TransportIndicesAliasesAction.TYPE), any(), any());

        verifyBulkIndexed("foobar", "Add the alias first", Level.INFO);
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

        assertBusy(() -> verify(client, times(1)).execute(eq(TransportPutComposableIndexTemplateAction.TYPE), any(), any()));
        assertBusy(() -> verify(client, times(1)).execute(eq(TransportCreateIndexAction.TYPE), any(), any()));

        // the back log will be written some point later
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        assertBusy(() -> verify(client, times(1)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any()));

        BulkRequest bulkRequest = bulkCaptor.getValue();
        assertThat(bulkRequest.numberOfActions(), equalTo(3));

        auditor.info("foobar", "Here is another message");
        verify(client, times(1)).execute(eq(TransportIndexAction.TYPE), any(), any());
    }

    public void testRecreateTemplateWhenDeleted() throws Exception {
        CountDownLatch writeSomeDocsBeforeTemplateLatch = new CountDownLatch(1);
        AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> auditor = createTestAuditorWithoutTemplate(
            writeSomeDocsBeforeTemplateLatch
        );

        auditor.info("foobar", "Here is my info to queue");

        verify(client, never()).execute(eq(TransportIndexAction.TYPE), any(), any());
        // fire the put template response
        writeSomeDocsBeforeTemplateLatch.countDown();

        assertBusy(() -> verify(client, times(1)).execute(eq(TransportPutComposableIndexTemplateAction.TYPE), any(), any()));
        assertBusy(() -> verify(client, times(1)).execute(eq(TransportCreateIndexAction.TYPE), any(), any()));

        // the back log will be written some point later
        assertBusy(() -> verify(client, times(1)).execute(eq(TransportBulkAction.TYPE), any(), any()));

        // "delete" the index
        doAnswer(ans -> {
            ActionListener<?> listener = ans.getArgument(2);
            listener.onFailure(new IndexNotFoundException("some index"));
            return null;
        }).when(client).execute(eq(TransportIndexAction.TYPE), any(), any());

        // audit more data
        auditor.info("foobar", "Here is another message");

        // verify the template is recreated and the audit message is processed
        assertBusy(() -> verify(client, times(2)).execute(eq(TransportPutComposableIndexTemplateAction.TYPE), any(), any()));
        assertBusy(() -> verify(client, times(2)).execute(eq(TransportCreateIndexAction.TYPE), any(), any()));
        assertBusy(() -> verify(client, times(2)).execute(eq(TransportBulkAction.TYPE), any(), any()));
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
        return new TestAuditor(client, TEST_NODE_NAME, mockClusterServiceAndIndexState(true));
    }

    @SuppressWarnings("unchecked")
    private TestAuditor createTestAuditorWithTemplateAndIndexButNoAlias() {
        doAnswer(invocationOnMock -> {
            ActionListener<IndicesAliasesResponse> listener = (ActionListener<IndicesAliasesResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(mock(IndicesAliasesResponse.class));
            return null;
        }).when(client).execute(eq(TransportIndicesAliasesAction.TYPE), any(), any());

        return new TestAuditor(client, TEST_NODE_NAME, mockClusterServiceAndIndexState(false));
    }

    private ClusterService mockClusterServiceAndIndexState(boolean includeAlias) {
        Map<String, IndexTemplateMetadata> templates = Map.of(TEST_INDEX_PREFIX, mock(IndexTemplateMetadata.class));
        var template = mock(ComposableIndexTemplate.class);
        when(template.version()).thenReturn((long) TEST_TEMPLATE_VERSION);
        Map<String, ComposableIndexTemplate> templatesV2 = Collections.singletonMap(TEST_INDEX_PREFIX, template);

        var indexMeta = Map.of(TEST_INDEX, createIndexMetadata(TEST_INDEX, includeAlias));
        Metadata metadata = Metadata.builder().indices(indexMeta).templates(templates).indexTemplates(templatesV2).build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static IndexMetadata createIndexMetadata(String indexName, boolean withAlias) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0));
        if (withAlias) {
            builder.putAlias(AliasMetadata.builder(TEST_INDEX_ALIAS).build());
        }
        return builder.build();
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

        doAnswer(invocationOnMock -> {
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(new CreateIndexResponse(true, true, "foo"));
            return null;
        }).when(client).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        doAnswer(invocationOnMock -> {
            ActionListener<ClusterHealthResponse> listener = (ActionListener<ClusterHealthResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(new ClusterHealthResponse());
            return null;
        }).when(client).execute(eq(TransportClusterHealthAction.TYPE), any(), any());

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        return new TestAuditor(client, TEST_NODE_NAME, clusterService);
    }

    public static class TestAuditor extends AbstractAuditor<AbstractAuditMessageTests.TestAuditMessage> {

        TestAuditor(Client client, String nodeName, ClusterService clusterService) {
            super(
                new OriginSettingClient(client, TEST_ORIGIN),
                TEST_INDEX_ALIAS,
                nodeName,
                AbstractAuditMessageTests.TestAuditMessage::new,
                clusterService,
                TestIndexNameExpressionResolver.newInstance(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected TransportPutComposableIndexTemplateAction.Request putTemplateRequest() {
            var templateConfig = new IndexTemplateConfig(
                TEST_INDEX_PREFIX,
                "/ml/notifications_index_template.json",
                TEST_TEMPLATE_VERSION,
                "xpack.ml.version",
                Map.of(
                    "xpack.ml.version.id",
                    String.valueOf(TEST_TEMPLATE_VERSION),
                    "xpack.ml.notifications.mappings",
                    NotificationsIndex.mapping()
                )
            );
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, templateConfig.loadBytes())) {
                return new TransportPutComposableIndexTemplateAction.Request(templateConfig.getTemplateName()).indexTemplate(
                    ComposableIndexTemplate.parse(parser)
                ).masterNodeTimeout(MASTER_TIMEOUT);
            } catch (IOException e) {
                throw new ElasticsearchParseException("unable to parse composable template " + templateConfig.getTemplateName(), e);
            }
        }

        @Override
        protected int templateVersion() {
            return TEST_TEMPLATE_VERSION;
        }

        @Override
        protected IndexDetails indexDetails() {
            return new IndexDetails(TEST_INDEX_PREFIX, TEST_INDEX_VERSION);
        }
    }
}
