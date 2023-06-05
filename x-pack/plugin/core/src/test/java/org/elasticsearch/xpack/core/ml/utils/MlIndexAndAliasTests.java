/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlIndexAndAliasTests extends ESTestCase {

    private static final String TEST_INDEX_PREFIX = "test";
    private static final String TEST_INDEX_ALIAS = "test-alias";
    private static final String LEGACY_INDEX_WITHOUT_SUFFIX = TEST_INDEX_PREFIX;
    private static final String FIRST_CONCRETE_INDEX = "test-000001";

    private ThreadPool threadPool;
    private IndicesAdminClient indicesAdminClient;
    private ClusterAdminClient clusterAdminClient;
    private AdminClient adminClient;
    private Client client;
    private ActionListener<Boolean> listener;

    private ArgumentCaptor<CreateIndexRequest> createRequestCaptor;
    private ArgumentCaptor<IndicesAliasesRequest> aliasesRequestCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.prepareCreate(FIRST_CONCRETE_INDEX)).thenReturn(
            new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE, FIRST_CONCRETE_INDEX)
        );
        doAnswer(withResponse(new CreateIndexResponse(true, true, FIRST_CONCRETE_INDEX))).when(indicesAdminClient).create(any(), any());
        when(indicesAdminClient.prepareAliases()).thenReturn(new IndicesAliasesRequestBuilder(client, IndicesAliasesAction.INSTANCE));
        doAnswer(withResponse(AcknowledgedResponse.TRUE)).when(indicesAdminClient).aliases(any(), any());
        doAnswer(withResponse(AcknowledgedResponse.TRUE)).when(indicesAdminClient).putTemplate(any(), any());

        clusterAdminClient = mock(ClusterAdminClient.class);
        doAnswer(invocationOnMock -> {
            ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) invocationOnMock
                .getArguments()[1];
            actionListener.onResponse(new ClusterHealthResponse());
            return null;
        }).when(clusterAdminClient).health(any(ClusterHealthRequest.class), any(ActionListener.class));

        adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client)
            .execute(
                any(PutComposableIndexTemplateAction.class),
                any(PutComposableIndexTemplateAction.Request.class),
                any(ActionListener.class)
            );

        listener = mock(ActionListener.class);

        createRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        aliasesRequestCaptor = ArgumentCaptor.forClass(IndicesAliasesRequest.class);
    }

    @After
    public void verifyNoMoreInteractionsWithMocks() {
        verifyNoMoreInteractions(indicesAdminClient, listener);
    }

    public void testInstallIndexTemplateIfRequired_GivenLegacyTemplateExistsAndModernCluster() throws UnknownHostException {
        ClusterState clusterState = createClusterState(
            Version.CURRENT,
            Collections.emptyMap(),
            Collections.singletonMap(
                NotificationsIndex.NOTIFICATIONS_INDEX,
                createLegacyIndexTemplateMetaData(
                    NotificationsIndex.NOTIFICATIONS_INDEX,
                    Collections.singletonList(NotificationsIndex.NOTIFICATIONS_INDEX)
                )
            ),
            Collections.emptyMap()
        );

        IndexTemplateConfig notificationsTemplate = new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
            "/org/elasticsearch/xpack/core/ml/notifications_index_template.json",
            Version.CURRENT.id,
            "xpack.ml.version",
            Map.of(
                "xpack.ml.version.id",
                String.valueOf(Version.CURRENT.id),
                "xpack.ml.notifications.mappings",
                NotificationsIndex.mapping()
            )
        );

        MlIndexAndAlias.installIndexTemplateIfRequired(
            clusterState,
            client,
            notificationsTemplate,
            TimeValue.timeValueMinutes(1),
            listener
        );
        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(same(PutComposableIndexTemplateAction.INSTANCE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    public void testInstallIndexTemplateIfRequired_GivenComposableTemplateExists() throws UnknownHostException {
        ClusterState clusterState = createClusterState(
            Version.CURRENT,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(
                NotificationsIndex.NOTIFICATIONS_INDEX,
                createComposableIndexTemplateMetaData(
                    NotificationsIndex.NOTIFICATIONS_INDEX,
                    Collections.singletonList(NotificationsIndex.NOTIFICATIONS_INDEX)
                )
            )
        );

        IndexTemplateConfig notificationsTemplate = new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
            "/org/elasticsearch/xpack/core/ml/notifications_index_template.json",
            Version.CURRENT.id,
            "xpack.ml.version",
            Map.of(
                "xpack.ml.version.id",
                String.valueOf(Version.CURRENT.id),
                "xpack.ml.notifications.mappings",
                NotificationsIndex.mapping()
            )
        );

        MlIndexAndAlias.installIndexTemplateIfRequired(
            clusterState,
            client,
            notificationsTemplate,
            TimeValue.timeValueMinutes(1),
            listener
        );
        verify(listener).onResponse(true);
        verifyNoMoreInteractions(client);
    }

    public void testInstallIndexTemplateIfRequired() throws UnknownHostException {
        ClusterState clusterState = createClusterState(Collections.emptyMap());

        IndexTemplateConfig notificationsTemplate = new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
            "/org/elasticsearch/xpack/core/ml/notifications_index_template.json",
            Version.CURRENT.id,
            "xpack.ml.version",
            Map.of(
                "xpack.ml.version.id",
                String.valueOf(Version.CURRENT.id),
                "xpack.ml.notifications.mappings",
                NotificationsIndex.mapping()
            )
        );

        MlIndexAndAlias.installIndexTemplateIfRequired(
            clusterState,
            client,
            notificationsTemplate,
            TimeValue.timeValueMinutes(1),
            listener
        );
        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(same(PutComposableIndexTemplateAction.INSTANCE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    public void testCreateStateIndexAndAliasIfNecessary_CleanState() throws UnknownHostException {
        ClusterState clusterState = createClusterState(Collections.emptyMap());
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareCreate(FIRST_CONCRETE_INDEX);
        inOrder.verify(indicesAdminClient).create(createRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        CreateIndexRequest createRequest = createRequestCaptor.getValue();
        assertThat(createRequest.index(), equalTo(FIRST_CONCRETE_INDEX));
        assertThat(createRequest.aliases(), equalTo(Collections.singleton(new Alias(TEST_INDEX_ALIAS).isHidden(true))));
    }

    private void assertNoClientInteractionsWhenWriteAliasAlreadyExists(String indexName) throws UnknownHostException {
        ClusterState clusterState = createClusterState(Collections.singletonMap(indexName, createIndexMetadataWithAlias(indexName)));
        createIndexAndAliasIfNecessary(clusterState);

        verify(listener).onResponse(false);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtInitialStateIndex() throws UnknownHostException {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists(FIRST_CONCRETE_INDEX);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtSubsequentStateIndex()
        throws UnknownHostException {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists("test-000007");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtDummyIndex() throws UnknownHostException {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists("dummy-index");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtLegacyStateIndex() throws UnknownHostException {
        ClusterState clusterState = createClusterState(
            Collections.singletonMap(LEGACY_INDEX_WITHOUT_SUFFIX, createIndexMetadataWithAlias(LEGACY_INDEX_WITHOUT_SUFFIX))
        );
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareCreate(FIRST_CONCRETE_INDEX);
        inOrder.verify(indicesAdminClient).create(createRequestCaptor.capture(), any());
        inOrder.verify(indicesAdminClient).prepareAliases();
        inOrder.verify(indicesAdminClient).aliases(aliasesRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        CreateIndexRequest createRequest = createRequestCaptor.getValue();
        assertThat(createRequest.index(), equalTo(FIRST_CONCRETE_INDEX));
        assertThat(createRequest.aliases(), empty());

        IndicesAliasesRequest indicesAliasesRequest = aliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(
                AliasActions.add().alias(TEST_INDEX_ALIAS).index(FIRST_CONCRETE_INDEX).isHidden(true),
                AliasActions.remove().alias(TEST_INDEX_ALIAS).index(LEGACY_INDEX_WITHOUT_SUFFIX)
            )
        );
    }

    private void assertMlStateWriteAliasAddedToMostRecentMlStateIndex(List<String> existingIndexNames, String expectedWriteIndexName)
        throws UnknownHostException {
        ClusterState clusterState = createClusterState(
            existingIndexNames.stream().collect(toMap(Function.identity(), MlIndexAndAliasTests::createIndexMetadata))
        );
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareAliases();
        inOrder.verify(indicesAdminClient).aliases(aliasesRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        IndicesAliasesRequest indicesAliasesRequest = aliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(AliasActions.add().alias(TEST_INDEX_ALIAS).index(expectedWriteIndexName).isHidden(true))
        );
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButInitialStateIndexExists() throws UnknownHostException {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(Arrays.asList(FIRST_CONCRETE_INDEX), FIRST_CONCRETE_INDEX);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButSubsequentStateIndicesExist() throws UnknownHostException {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(Arrays.asList("test-000003", "test-000040", "test-000500"), "test-000500");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButBothLegacyAndNewIndicesExist()
        throws UnknownHostException {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(
            Arrays.asList(LEGACY_INDEX_WITHOUT_SUFFIX, "test-000003", "test-000040", "test-000500"),
            "test-000500"
        );
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButLegacyStateIndexExists() throws UnknownHostException {
        ClusterState clusterState = createClusterState(
            Collections.singletonMap(LEGACY_INDEX_WITHOUT_SUFFIX, createIndexMetadata(LEGACY_INDEX_WITHOUT_SUFFIX))
        );
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareCreate(FIRST_CONCRETE_INDEX);
        inOrder.verify(indicesAdminClient).create(createRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        CreateIndexRequest createRequest = createRequestCaptor.getValue();
        assertThat(createRequest.index(), equalTo(FIRST_CONCRETE_INDEX));
        assertThat(createRequest.aliases(), equalTo(Collections.singleton(new Alias(TEST_INDEX_ALIAS).isHidden(true))));
    }

    public void testIndexNameComparator() {
        Comparator<String> comparator = MlIndexAndAlias.INDEX_NAME_COMPARATOR;
        assertThat(Stream.of("test-000001").max(comparator).get(), equalTo("test-000001"));
        assertThat(Stream.of("test-000002", "test-000001").max(comparator).get(), equalTo("test-000002"));
        assertThat(Stream.of("test-000003", "test-000040", "test-000500").max(comparator).get(), equalTo("test-000500"));
        assertThat(Stream.of("test-000042", "test-000049", "test-000038").max(comparator).get(), equalTo("test-000049"));
        assertThat(Stream.of("test", "test-000003", "test-000040", "test-000500").max(comparator).get(), equalTo("test-000500"));
        assertThat(Stream.of(".reindexed-6-test", "test-000042").max(comparator).get(), equalTo("test-000042"));
        assertThat(Stream.of(".a-000002", ".b-000001").max(comparator).get(), equalTo(".a-000002"));
    }

    private void createIndexAndAliasIfNecessary(ClusterState clusterState) {
        MlIndexAndAlias.createIndexAndAliasIfNecessary(
            client,
            clusterState,
            TestIndexNameExpressionResolver.newInstance(),
            TEST_INDEX_PREFIX,
            TEST_INDEX_ALIAS,
            MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            listener
        );
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[1];
            listener.onResponse(response);
            return null;
        };
    }

    private static ClusterState createClusterState(Map<String, IndexMetadata> indices) throws UnknownHostException {
        return createClusterState(Version.CURRENT, indices, Collections.emptyMap(), Collections.emptyMap());
    }

    private static ClusterState createClusterState(
        Version minNodeVersion,
        Map<String, IndexMetadata> indices,
        Map<String, IndexTemplateMetadata> legacyTemplates,
        Map<String, ComposableIndexTemplate> composableTemplates
    ) throws UnknownHostException {
        InetAddress inetAddress1 = InetAddress.getByAddress(new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        InetAddress inetAddress2 = InetAddress.getByAddress(new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 2 });
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("foo", new TransportAddress(inetAddress1, 9201)))
                    .add(DiscoveryNodeUtils.create("bar", new TransportAddress(inetAddress2, 9202), minNodeVersion))
                    .build()
            )
            .metadata(Metadata.builder().indices(indices).templates(legacyTemplates).indexTemplates(composableTemplates).build())
            .build();
    }

    private static IndexMetadata createIndexMetadata(String indexName) {
        return createIndexMetadata(indexName, false);
    }

    private static IndexMetadata createIndexMetadataWithAlias(String indexName) {
        return createIndexMetadata(indexName, true);
    }

    private static IndexTemplateMetadata createLegacyIndexTemplateMetaData(String templateName, List<String> patterns) {
        return IndexTemplateMetadata.builder(templateName).patterns(patterns).build();
    }

    private static ComposableIndexTemplate createComposableIndexTemplateMetaData(String templateName, List<String> patterns) {
        return new ComposableIndexTemplate.Builder().indexPatterns(patterns).build();
    }

    private static IndexMetadata createIndexMetadata(String indexName, boolean withAlias) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName).settings(indexSettings(Version.CURRENT, 1, 0));
        if (withAlias) {
            builder.putAlias(AliasMetadata.builder(TEST_INDEX_ALIAS).build());
        }
        return builder.build();
    }
}
