/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.xpack.logstash.LogstashTemplateRegistry.LOGSTASH_INDEX_NAME;
import static org.elasticsearch.xpack.logstash.LogstashTemplateRegistry.LOGSTASH_TEMPLATE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogstashTemplateRegistryTests extends ESTestCase {
    private static final int NUM_LOGSTASH_INDEXES = 1; // .logstash

    private InternalClient client;
    private ExecutorService executorService;
    private TransportClient transportClient;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private LogstashTemplateRegistry logstashTemplateRegistry;
    private static final ClusterState EMPTY_CLUSTER_STATE =
        new ClusterState.Builder(new ClusterName("test-cluster")).build();
    CopyOnWriteArrayList<ActionListener> listeners;

    @Before
    public void setup() {
        executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);

        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        transportClient = new MockTransportClient(Settings.EMPTY);
        class TestInternalClient extends InternalClient {
            TestInternalClient(Client transportClient) {
                super(Settings.EMPTY, null, transportClient);
            }

            @Override
            protected <Request extends ActionRequest,
                Response extends ActionResponse,
                RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            void doExecute(Action<Request, Response, RequestBuilder> action, Request request,
                           ActionListener<Response> listener) {
                listeners.add(listener);
            }
        }
        client = new TestInternalClient(transportClient);
        listeners = new CopyOnWriteArrayList<>();
        logstashTemplateRegistry = new LogstashTemplateRegistry(Settings.EMPTY, clusterService, client);
    }

    @After
    public void stop() throws InterruptedException {
        if (transportClient != null) {
            transportClient.close();
        }
    }

    public void testAddsListener() throws Exception {
        LogstashTemplateRegistry templateRegistry = new LogstashTemplateRegistry(Settings.EMPTY, clusterService, client);
        verify(clusterService, times(1)).addListener(templateRegistry);
    }

    public void testAddTemplatesIfMissing() throws IOException {
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(
            "/" + LOGSTASH_TEMPLATE_NAME + ".json"
        );
        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(true));
        assertThat(listeners, hasSize(0));
    }

    public void testWrongVersionIndexTemplate_isIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/wrong-version-" + LOGSTASH_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);

        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat(listeners, hasSize(NUM_LOGSTASH_INDEXES));
    }

    public void testWrongVersionIndexTemplate_isUpdated() throws IOException {
        String templateString = "/wrong-version-" + LOGSTASH_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);

        final ClusterState clusterState = clusterStateBuilder.build();
        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterState, EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat(listeners, hasSize(NUM_LOGSTASH_INDEXES));
        assertThat("Expected pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(true));

        // if we do it again this should not send an update
        ActionListener listener = listeners.get(0);
        listeners.clear();
        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterState, EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat(listeners, hasSize(0));
        assertThat("Expected pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(true));

        // if we now simulate an error...
        listener.onFailure(new Exception());
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertFalse(logstashTemplateRegistry.isTemplateCreationPending());

        // ... we should be able to send a new update
        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterState, EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat(listeners, hasSize(1));
        assertThat("Expected pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(true));

        // now check what happens if we get back an unacknowledged response
        listeners.get(0).onResponse(new TestPutIndexTemplateResponse());
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat("Didn't expect pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(false));

        // and now let's see what happens if we get back a response
        listeners.clear();
        logstashTemplateRegistry.clusterChanged(new ClusterChangedEvent("test-event",
            clusterState, EMPTY_CLUSTER_STATE));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(false));
        assertThat("Expected pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(true));
        assertThat(listeners, hasSize(1));
        listeners.get(0).onResponse(new TestPutIndexTemplateResponse(true));
        assertThat(logstashTemplateRegistry.isTemplateUpToDate(), equalTo(true));
        assertThat("Didn't expect pending template creation", logstashTemplateRegistry.isTemplateCreationPending(), is(false));
    }

    private static ClusterState.Builder createClusterStateWithTemplate(String logstashTemplateString) throws IOException {
        MetaData.Builder metaDataBuilder = new MetaData.Builder();

        IndexTemplateMetaData.Builder logstashTemplateBuilder =
            getIndexTemplateMetaData(LOGSTASH_TEMPLATE_NAME, logstashTemplateString);
        metaDataBuilder.put(logstashTemplateBuilder);
        // add the correct mapping no matter what the template
        String logstashMappingString = "/" + LOGSTASH_TEMPLATE_NAME + ".json";
        IndexMetaData.Builder logstashIndexMeta =
            createIndexMetadata(LOGSTASH_INDEX_NAME, logstashMappingString);
        metaDataBuilder.put(logstashIndexMeta);

        return ClusterState.builder(state()).metaData(metaDataBuilder.build());
    }

    private static IndexTemplateMetaData.Builder getIndexTemplateMetaData(
        String templateName, String templateString) throws IOException {

        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString(),
            LogstashTemplateRegistry.TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexTemplateMetaData.Builder templateBuilder =
            IndexTemplateMetaData.builder(templateName);
        for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
            templateBuilder.putMapping(entry.getKey(), entry.getValue());
        }
        return templateBuilder;
    }

    private static IndexMetaData.Builder createIndexMetadata(
        String indexName, String templateString) throws IOException {
        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString(),
            LogstashTemplateRegistry.TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
        indexMetaData.settings(Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build());

        for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
            indexMetaData.putMapping(entry.getKey(), entry.getValue());
        }
        return indexMetaData;
    }

    // cluster state where local node is master
    private static ClusterState state() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.masterNodeId("1");
        discoBuilder.localNodeId("1");
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test-cluster"));
        state.nodes(discoBuilder);
        state.metaData(MetaData.builder().generateClusterUuidIfNeeded());
        return state.build();
    }

    private static class TestPutIndexTemplateResponse extends PutIndexTemplateResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }

        TestPutIndexTemplateResponse() {
            super();
        }
    }
}
