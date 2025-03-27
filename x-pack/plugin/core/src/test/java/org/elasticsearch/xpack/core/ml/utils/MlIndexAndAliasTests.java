/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActiveShardCount;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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

    private static final int TEST_TEMPLATE_VERSION = 12345678;

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
            new CreateIndexRequestBuilder(client, FIRST_CONCRETE_INDEX)
        );
        doAnswer(withResponse(new CreateIndexResponse(true, true, FIRST_CONCRETE_INDEX))).when(indicesAdminClient).create(any(), any());
        when(indicesAdminClient.prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).thenReturn(
            new IndicesAliasesRequestBuilder(client, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
        );
        doAnswer(withResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS)).when(indicesAdminClient).aliases(any(), any());
        doAnswer(withResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS)).when(indicesAdminClient).putTemplate(any(), any());

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
            ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) invocationOnMock
                .getArguments()[2];
            actionListener.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
            return null;
        }).when(client)
            .execute(
                same(TransportPutComposableIndexTemplateAction.TYPE),
                any(TransportPutComposableIndexTemplateAction.Request.class),
                any(ActionListener.class)
            );

        listener = mock(ActionListener.class);
        when(listener.delegateFailureAndWrap(any())).thenCallRealMethod();

        createRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        aliasesRequestCaptor = ArgumentCaptor.forClass(IndicesAliasesRequest.class);
    }

    @After
    public void verifyNoMoreInteractionsWithMocks() {
        verifyNoMoreInteractions(indicesAdminClient, listener);
    }

    public void testInstallIndexTemplateIfRequired_GivenLegacyTemplateExistsAndModernCluster() {
        ClusterState clusterState = createClusterState(
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

        MlIndexAndAlias.installIndexTemplateIfRequired(
            clusterState,
            client,
            notificationsTemplate,
            TimeValue.timeValueMinutes(1),
            listener
        );
        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(listener).delegateFailureAndWrap(any());
        inOrder.verify(client).execute(same(TransportPutComposableIndexTemplateAction.TYPE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    public void testInstallIndexTemplateIfRequired_GivenComposableTemplateExists() {
        ClusterState clusterState = createClusterState(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(
                NotificationsIndex.NOTIFICATIONS_INDEX,
                createComposableIndexTemplateMetaData(
                    NotificationsIndex.NOTIFICATIONS_INDEX,
                    Collections.singletonList(NotificationsIndex.NOTIFICATIONS_INDEX),
                    TEST_TEMPLATE_VERSION
                )
            )
        );

        IndexTemplateConfig notificationsTemplate = new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
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

    public void testInstallIndexTemplateIfRequired() {
        ClusterState clusterState = createClusterState(Collections.emptyMap());

        IndexTemplateConfig notificationsTemplate = new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
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

        MlIndexAndAlias.installIndexTemplateIfRequired(
            clusterState,
            client,
            notificationsTemplate,
            TimeValue.timeValueMinutes(1),
            listener
        );
        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(listener).delegateFailureAndWrap(any());
        inOrder.verify(client).execute(same(TransportPutComposableIndexTemplateAction.TYPE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    public void testCreateStateIndexAndAliasIfNecessary_CleanState() {
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

    private void assertNoClientInteractionsWhenWriteAliasAlreadyExists(String indexName) {
        ClusterState clusterState = createClusterState(Collections.singletonMap(indexName, createIndexMetadataWithAlias(indexName)));
        createIndexAndAliasIfNecessary(clusterState);

        verify(listener).onResponse(false);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtInitialStateIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists(FIRST_CONCRETE_INDEX);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtSubsequentStateIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists("test-000007");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtDummyIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists("dummy-index");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtLegacyStateIndex() {
        ClusterState clusterState = createClusterState(
            Collections.singletonMap(LEGACY_INDEX_WITHOUT_SUFFIX, createIndexMetadataWithAlias(LEGACY_INDEX_WITHOUT_SUFFIX))
        );
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareCreate(FIRST_CONCRETE_INDEX);
        inOrder.verify(indicesAdminClient).create(createRequestCaptor.capture(), any());
        inOrder.verify(indicesAdminClient).prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        inOrder.verify(indicesAdminClient).aliases(aliasesRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        CreateIndexRequest createRequest = createRequestCaptor.getValue();
        assertThat(createRequest.index(), equalTo(FIRST_CONCRETE_INDEX));
        assertThat(createRequest.aliases(), empty());

        IndicesAliasesRequest indicesAliasesRequest = aliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(
                AliasActions.add().alias(TEST_INDEX_ALIAS).index(FIRST_CONCRETE_INDEX).isHidden(true).writeIndex(true),
                AliasActions.remove().alias(TEST_INDEX_ALIAS).index(LEGACY_INDEX_WITHOUT_SUFFIX)
            )
        );
    }

    private void assertMlStateWriteAliasAddedToMostRecentMlStateIndex(List<String> existingIndexNames, String expectedWriteIndexName) {
        ClusterState clusterState = createClusterState(
            existingIndexNames.stream().collect(toMap(Function.identity(), MlIndexAndAliasTests::createIndexMetadata))
        );
        createIndexAndAliasIfNecessary(clusterState);

        InOrder inOrder = inOrder(indicesAdminClient, listener);
        inOrder.verify(indicesAdminClient).prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        inOrder.verify(indicesAdminClient).aliases(aliasesRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(true);

        IndicesAliasesRequest indicesAliasesRequest = aliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(AliasActions.add().alias(TEST_INDEX_ALIAS).index(expectedWriteIndexName).isHidden(true).writeIndex(true))
        );
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButInitialStateIndexExists() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(Arrays.asList(FIRST_CONCRETE_INDEX), FIRST_CONCRETE_INDEX);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButSubsequentStateIndicesExist() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(Arrays.asList("test-000003", "test-000040", "test-000500"), "test-000500");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButBothLegacyAndNewIndicesExist() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(
            Arrays.asList(LEGACY_INDEX_WITHOUT_SUFFIX, "test-000003", "test-000040", "test-000500"),
            "test-000500"
        );
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButLegacyStateIndexExists() {
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

    public void testLatestIndex() {
        {
            var names = new String[] { "index-000001", "index-000002", "index-000003" };
            assertThat(MlIndexAndAlias.latestIndex(names), equalTo("index-000003"));
        }
        {
            var names = new String[] { "index", "index-000001", "index-000002" };
            assertThat(MlIndexAndAlias.latestIndex(names), equalTo("index-000002"));
        }
    }

    public void testIndexIsReadWriteCompatibleInV9() {
        assertTrue(MlIndexAndAlias.indexIsReadWriteCompatibleInV9(IndexVersion.current()));
        assertTrue(MlIndexAndAlias.indexIsReadWriteCompatibleInV9(IndexVersions.V_8_0_0));
        assertFalse(MlIndexAndAlias.indexIsReadWriteCompatibleInV9(IndexVersions.V_7_17_0));
    }

    public void testHas6DigitSuffix() {
        assertTrue(MlIndexAndAlias.has6DigitSuffix("index-000001"));
        assertFalse(MlIndexAndAlias.has6DigitSuffix("index1"));
        assertFalse(MlIndexAndAlias.has6DigitSuffix("index-foo"));
        assertFalse(MlIndexAndAlias.has6DigitSuffix("index000001"));
    }

    private void createIndexAndAliasIfNecessary(ClusterState clusterState) {
        MlIndexAndAlias.createIndexAndAliasIfNecessary(
            client,
            clusterState,
            TestIndexNameExpressionResolver.newInstance(),
            TEST_INDEX_PREFIX,
            TEST_INDEX_ALIAS,
            TimeValue.timeValueSeconds(30),
            ActiveShardCount.DEFAULT,
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

    private static ClusterState createClusterState(Map<String, IndexMetadata> indices) {
        return createClusterState(indices, Collections.emptyMap(), Collections.emptyMap());
    }

    private static ClusterState createClusterState(
        Map<String, IndexMetadata> indices,
        Map<String, IndexTemplateMetadata> legacyTemplates,
        Map<String, ComposableIndexTemplate> composableTemplates
    ) {
        return ClusterState.builder(ClusterName.DEFAULT)
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

    private static ComposableIndexTemplate createComposableIndexTemplateMetaData(String templateName, List<String> patterns, long version) {
        return ComposableIndexTemplate.builder().indexPatterns(patterns).version(version).build();
    }

    private static IndexMetadata createIndexMetadata(String indexName, boolean withAlias) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0));
        if (withAlias) {
            builder.putAlias(AliasMetadata.builder(TEST_INDEX_ALIAS).build());
        }
        return builder.build();
    }
}
