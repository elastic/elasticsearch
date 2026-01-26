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
import org.elasticsearch.client.internal.ElasticsearchClient;
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
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
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
import static org.hamcrest.Matchers.hasSize;
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

    private IndicesAdminClient indicesAdminClient;
    private Client client;
    private ActionListener<Boolean> listener;

    private ArgumentCaptor<CreateIndexRequest> createRequestCaptor;
    private ArgumentCaptor<IndicesAliasesRequest> aliasesRequestCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        final ThreadPool threadPool = mock(ThreadPool.class);
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

        final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        doAnswer(invocationOnMock -> {
            ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) invocationOnMock
                .getArguments()[1];
            actionListener.onResponse(new ClusterHealthResponse());
            return null;
        }).when(clusterAdminClient).health(any(ClusterHealthRequest.class), any(ActionListener.class));

        final AdminClient adminClient = mock(AdminClient.class);
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
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(List.of(FIRST_CONCRETE_INDEX), FIRST_CONCRETE_INDEX);
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

    public void testIsAnomaliesWriteAlias() {
        assertTrue(MlIndexAndAlias.isAnomaliesWriteAlias(AnomalyDetectorsIndex.resultsWriteAlias("foo")));
        assertFalse(MlIndexAndAlias.isAnomaliesWriteAlias(AnomalyDetectorsIndex.jobResultsAliasedName("foo")));
        assertFalse(MlIndexAndAlias.isAnomaliesWriteAlias("some-index"));
    }

    public void testIsAnomaliesAlias() {
        assertTrue(MlIndexAndAlias.isAnomaliesReadAlias(AnomalyDetectorsIndex.jobResultsAliasedName("foo")));
        assertFalse(MlIndexAndAlias.isAnomaliesReadAlias(AnomalyDetectorsIndex.resultsWriteAlias("foo")));
        assertFalse(MlIndexAndAlias.isAnomaliesReadAlias("some-index"));
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

    public void testIsAnomaliesSharedIndex() {
        assertTrue(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-000001"));
        assertTrue(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-000007"));
        assertTrue(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-100000"));
        assertTrue(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-999999"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-1000000"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared-00001"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-custom-fred-000007"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex("shared-000007"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-shared000007"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-state-000007"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-annotations-000007"));
        assertFalse(MlIndexAndAlias.isAnomaliesSharedIndex(".ml-anomalies-stats-000007"));
    }

    public void testCreateRolloverAliasAndNewIndexName() {
        var alias_index1 = MlIndexAndAlias.createRolloverAliasAndNewIndexName("fred");
        assertThat(alias_index1.v1(), equalTo("fred" + MlIndexAndAlias.ROLLOVER_ALIAS_SUFFIX));
        assertThat(alias_index1.v2(), equalTo("fred" + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX));

        var alias_index2 = MlIndexAndAlias.createRolloverAliasAndNewIndexName("derf" + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX);
        assertThat(
            alias_index2.v1(),
            equalTo("derf" + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX + MlIndexAndAlias.ROLLOVER_ALIAS_SUFFIX)
        );
        assertThat(alias_index2.v2(), equalTo(null));

        assertThrows(NullPointerException.class, () -> MlIndexAndAlias.createRolloverAliasAndNewIndexName(null));
    }

    public void testLatestIndexMatchingBaseName_isLatest() {
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo", IndexVersion.current(), List.of("job1")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-bar", IndexVersion.current(), List.of("job2")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-bax", IndexVersion.current(), List.of("job3")));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var latest = MlIndexAndAlias.latestIndexMatchingBaseName(
            ".ml-anomalies-custom-foo",
            TestIndexNameExpressionResolver.newInstance(),
            csBuilder.build()
        );
        assertEquals(".ml-anomalies-custom-foo", latest);
    }

    public void testLatestIndexMatchingBaseName_hasLater() {
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo", IndexVersion.current(), List.of("job1")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-bar", IndexVersion.current(), List.of("job2")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo-000001", IndexVersion.current(), List.of("job3")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo-000002", IndexVersion.current(), List.of("job4")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-baz-000001", IndexVersion.current(), List.of("job5")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-baz-000002", IndexVersion.current(), List.of("job6")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-baz-000003", IndexVersion.current(), List.of("job7")));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);
        var state = csBuilder.build();

        assertTrue(MlIndexAndAlias.has6DigitSuffix(".ml-anomalies-custom-foo-000002"));

        var latest = MlIndexAndAlias.latestIndexMatchingBaseName(
            ".ml-anomalies-custom-foo",
            TestIndexNameExpressionResolver.newInstance(),
            state
        );
        assertEquals(".ml-anomalies-custom-foo-000002", latest);

        latest = MlIndexAndAlias.latestIndexMatchingBaseName(
            ".ml-anomalies-custom-baz-000001",
            TestIndexNameExpressionResolver.newInstance(),
            state
        );
        assertEquals(".ml-anomalies-custom-baz-000003", latest);
    }

    public void testLatestIndexMatchingBaseName_CollidingIndexNames() {
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo", IndexVersion.current(), List.of("job1")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-bar", IndexVersion.current(), List.of("job2")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foodifferent000001", IndexVersion.current(), List.of("job3")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo-notthisone-000001", IndexVersion.current(), List.of("job4")));
        metadata.put(createSharedResultsIndex(".ml-anomalies-custom-foo-notthisone-000002", IndexVersion.current(), List.of("job5")));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);
        var state = csBuilder.build();

        var latest = MlIndexAndAlias.latestIndexMatchingBaseName(
            ".ml-anomalies-custom-foo",
            TestIndexNameExpressionResolver.newInstance(),
            state
        );
        assertEquals(".ml-anomalies-custom-foo", latest);

        latest = MlIndexAndAlias.latestIndexMatchingBaseName(
            ".ml-anomalies-custom-foo-notthisone-000001",
            TestIndexNameExpressionResolver.newInstance(),
            state
        );
        assertEquals(".ml-anomalies-custom-foo-notthisone-000002", latest);
    }

    public void testBuildIndexAliasesRequest() {
        var anomaliesIndex = ".ml-anomalies-sharedindex";
        var newIndex = anomaliesIndex + "-000001";

        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder oldIndexMetadata = createSharedResultsIndex(anomaliesIndex, IndexVersion.current(), jobs);
        IndexMetadata.Builder newIndexMetadata = createEmptySharedResultsIndex(newIndex, IndexVersion.current());

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(oldIndexMetadata);
        metadata.put(newIndexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        IndicesAliasesRequestBuilder aliasRequestBuilder = new IndicesAliasesRequestBuilder(
            mock(ElasticsearchClient.class),
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT
        );

        String[] currentIndices = { anomaliesIndex };
        var request = MlIndexAndAlias.addResultsIndexRolloverAliasActions(
            aliasRequestBuilder,
            newIndex,
            csBuilder.build(),
            Arrays.asList(currentIndices)
        );
        var actions = request.request().getAliasActions();
        assertThat(actions, hasSize(6));

        // The order in which the alias actions are created
        // is not preserved so look for the item in the list
        for (var job : jobs) {
            var expected = new AliasActionMatcher(
                AnomalyDetectorsIndex.resultsWriteAlias(job),
                newIndex,
                IndicesAliasesRequest.AliasActions.Type.ADD
            );

            assertThat(actions.stream().filter(expected::matches).count(), equalTo(1L));

            expected = new AliasActionMatcher(
                AnomalyDetectorsIndex.resultsWriteAlias(job),
                anomaliesIndex,
                IndicesAliasesRequest.AliasActions.Type.REMOVE
            );
            assertThat(actions.stream().filter(expected::matches).count(), equalTo(1L));

            // This alias action request ensures that every index has a read alias, even if the old index was missing one.
            var expected1 = new AliasActionMultiIndicesMatcher(
                AnomalyDetectorsIndex.jobResultsAliasedName(job),
                new String[] { anomaliesIndex, newIndex },
                IndicesAliasesRequest.AliasActions.Type.ADD
            );
            assertThat(actions.stream().filter(expected1::matches).count(), equalTo(1L));
        }
    }

    private record AliasActionMatcher(String aliasName, String index, IndicesAliasesRequest.AliasActions.Type actionType) {
        boolean matches(IndicesAliasesRequest.AliasActions aliasAction) {
            return aliasAction.actionType() == actionType
                && aliasAction.aliases()[0].equals(aliasName)
                && aliasAction.indices()[0].equals(index);
        }
    }

    private record AliasActionMultiIndicesMatcher(String aliasName, String[] indices, IndicesAliasesRequest.AliasActions.Type actionType) {
        boolean matches(IndicesAliasesRequest.AliasActions aliasAction) {
            return aliasAction.actionType() == actionType
                && aliasAction.aliases()[0].equals(aliasName)
                && Arrays.stream(aliasAction.indices()).toList().equals(Arrays.stream(indices).toList());
        }
    }

    private IndexMetadata.Builder createEmptySharedResultsIndex(String indexName, IndexVersion indexVersion) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        return indexMetadata;
    }

    private IndexMetadata.Builder createSharedResultsIndex(String indexName, IndexVersion indexVersion, List<String> jobs) {

        var indexMetadata = createEmptySharedResultsIndex(indexName, indexVersion);

        for (var jobId : jobs) {
            indexMetadata.putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).isHidden(true).build());
            indexMetadata.putAlias(
                AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).writeIndex(true).isHidden(true).build()
            );
        }

        return indexMetadata;
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
