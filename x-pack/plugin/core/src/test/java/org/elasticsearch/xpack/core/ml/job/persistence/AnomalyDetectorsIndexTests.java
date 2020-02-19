/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnomalyDetectorsIndexTests extends ESTestCase {

    private static final String ML_STATE = ".ml-state";
    private static final String ML_STATE_WRITE_ALIAS = ".ml-state-write";

    private ThreadPool threadPool;
    private IndicesAdminClient indicesAdminClient;
    private AdminClient adminClient;
    private Client client;
    private ActionListener<Boolean> finalListener;

    private ArgumentCaptor<CreateIndexRequest> createRequestCaptor;
    private ArgumentCaptor<IndicesAliasesRequest> aliasesRequestCaptor;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.prepareCreate(ML_STATE))
            .thenReturn(new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE, ML_STATE));
        doAnswer(withResponse(new CreateIndexResponse(true, true, ML_STATE))).when(indicesAdminClient).create(any(), any());
        when(indicesAdminClient.prepareAliases()).thenReturn(new IndicesAliasesRequestBuilder(client, IndicesAliasesAction.INSTANCE));
        doAnswer(withResponse(new AcknowledgedResponse(true))).when(indicesAdminClient).aliases(any(), any());

        adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.admin()).thenReturn(adminClient);

        finalListener = mock(ActionListener.class);

        createRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        aliasesRequestCaptor = ArgumentCaptor.forClass(IndicesAliasesRequest.class);
    }

    @After
    public void verifyNoMoreInteractionsWithMocks() {
        verifyNoMoreInteractions(indicesAdminClient, finalListener);
    }

    public void testCreateStateIndexAndAliasIfNecessary_CleanState() {
        ClusterState clusterState = createClusterState(Collections.emptyMap());
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, finalListener);

        InOrder inOrder = inOrder(indicesAdminClient, finalListener);
        inOrder.verify(indicesAdminClient).prepareCreate(ML_STATE);
        inOrder.verify(indicesAdminClient).create(createRequestCaptor.capture(), any());
        inOrder.verify(finalListener).onResponse(true);

        CreateIndexRequest createRequest = createRequestCaptor.getValue();
        assertThat(createRequest.index(), equalTo(ML_STATE));
        assertThat(createRequest.aliases(), equalTo(Collections.singleton(new Alias(ML_STATE_WRITE_ALIAS))));
    }

    private void assertNoClientInteractionsWhenWriteAliasAlreadyExists(String indexName) {
        ClusterState clusterState = createClusterState(Collections.singletonMap(indexName, createIndexMetaDataWithAlias(indexName)));
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, finalListener);

        verify(finalListener).onResponse(false);
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtInitialStateIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists(".ml-state-000001");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtSubsequentStateIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists(".ml-state-000007");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasAlreadyExistsAndPointsAtDummyIndex() {
        assertNoClientInteractionsWhenWriteAliasAlreadyExists("dummy-index");
    }

    private void assertMlStateWriteAliasAddedToMostRecentMlStateIndex(List<String> existingIndexNames, String expectedWriteIndexName) {
        ClusterState clusterState =
            createClusterState(
                existingIndexNames.stream().collect(toMap(Function.identity(), AnomalyDetectorsIndexTests::createIndexMetaData)));
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, finalListener);

        InOrder inOrder = inOrder(indicesAdminClient, finalListener);
        inOrder.verify(indicesAdminClient).prepareAliases();
        inOrder.verify(indicesAdminClient).aliases(aliasesRequestCaptor.capture(), any());
        inOrder.verify(finalListener).onResponse(true);

        IndicesAliasesRequest indicesAliasesRequest = aliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(AliasActions.add().alias(ML_STATE_WRITE_ALIAS).index(expectedWriteIndexName)));
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButInitialStateIndexExists() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(
            Arrays.asList(".ml-state-000001"), ".ml-state-000001");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButSubsequentStateIndicesExist() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(
            Arrays.asList(".ml-state-000003", ".ml-state-000040", ".ml-state-000500"), ".ml-state-000500");
    }

    public void testCreateStateIndexAndAliasIfNecessary_WriteAliasDoesNotExistButBothLegacyAndNewStateIndicesDoExist() {
        assertMlStateWriteAliasAddedToMostRecentMlStateIndex(
            Arrays.asList(ML_STATE, ".ml-state-000003", ".ml-state-000040", ".ml-state-000500"), ".ml-state-000500");
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[1];
            listener.onResponse(response);
            return null;
        };
    }

    private static ClusterState createClusterState(Map<String, IndexMetaData> indices) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .indices(ImmutableOpenMap.<String, IndexMetaData>builder().putAll(indices).build()).build())
            .build();
    }

    private static IndexMetaData createIndexMetaData(String indexName) {
        return createIndexMetaData(indexName, false);
    }

    private static IndexMetaData createIndexMetaDataWithAlias(String indexName) {
        return createIndexMetaData(indexName, true);
    }

    private static IndexMetaData createIndexMetaData(String indexName, boolean withAlias) {
        Settings settings =
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                .build();
        IndexMetaData.Builder builder = IndexMetaData.builder(indexName)
            .settings(settings);
        if (withAlias) {
            builder.putAlias(AliasMetaData.builder(ML_STATE_WRITE_ALIAS).build());
        }
        return builder.build();
    }
}
