/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.user.User;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class CcrLicenseCheckerTests extends ESTestCase {

    public void testNoAuthenticationInfo() {
        final boolean isCcrAllowed = randomBoolean();
        final CcrLicenseChecker checker = new CcrLicenseChecker(() -> isCcrAllowed, () -> true) {

            @Override
            User getUser(final ThreadContext threadContext) {
                return null;
            }

        };
        final AtomicBoolean invoked = new AtomicBoolean();
        checker.hasPrivilegesToFollowIndices(
            new ThreadContext(Settings.EMPTY),
            mock(RemoteClusterClient.class),
            new String[] { randomAlphaOfLength(8) },
            e -> {
                invoked.set(true);
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(e, hasToString(containsString("missing or unable to read authentication info on request")));
            }
        );
        assertTrue(invoked.get());
    }

    /**
     * Tests all validation logic after obtaining the remote cluster state and before executing the check for follower privileges.
     */
    public void testRemoteIndexValidation() {
        // A cluster state with
        // - a data stream, containing a backing index and a failure index
        // - an alias that points to said data stream
        // - a standalone index
        // - an alias that points to said standalone index
        // - a closed index
        String indexName = "random-index";
        String closedIndexName = "closed-index";
        String dataStreamName = "logs-test-data";
        String aliasName = "foo-alias";
        String dsAliasName = "ds-alias";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .putAlias(AliasMetadata.builder(aliasName))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(5)
            .numberOfReplicas(1)
            .build();
        IndexMetadata closedIndexMetadata = IndexMetadata.builder(closedIndexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(5)
            .numberOfReplicas(1)
            .state(IndexMetadata.State.CLOSE)
            .build();
        IndexMetadata firstBackingIndex = DataStreamTestHelper.createFirstBackingIndex(dataStreamName).build();
        IndexMetadata firstFailureStore = DataStreamTestHelper.createFirstFailureStore(dataStreamName).build();
        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(firstBackingIndex.getIndex()),
            List.of(firstFailureStore.getIndex())
        );
        ClusterState remoteClusterState = ClusterState.builder(new ClusterName(randomIdentifier()))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, false)
                    .put(closedIndexMetadata, false)
                    .put(firstBackingIndex, false)
                    .put(firstFailureStore, false)
                    .dataStreams(
                        Map.of(dataStreamName, dataStream),
                        Map.of(dsAliasName, new DataStreamAlias(dsAliasName, List.of(dataStreamName), dataStreamName, Map.of()))
                    )
            )
            .build();

        final boolean isCcrAllowed = randomBoolean();
        final CcrLicenseChecker checker = new CcrLicenseChecker(() -> isCcrAllowed, () -> true) {
            @Override
            User getUser(ThreadContext threadContext) {
                return null;
            }

            @Override
            protected void doCheckRemoteClusterLicenseAndFetchClusterState(
                Client client,
                String clusterAlias,
                RemoteClusterClient remoteClient,
                ClusterStateRequest request,
                Consumer<Exception> onFailure,
                Consumer<ClusterStateResponse> leaderClusterStateConsumer,
                Function<RemoteClusterLicenseChecker.LicenseCheck, ElasticsearchStatusException> nonCompliantLicense,
                Function<Exception, ElasticsearchStatusException> unknownLicense
            ) {
                leaderClusterStateConsumer.accept(new ClusterStateResponse(remoteClusterState.getClusterName(), remoteClusterState, false));
            }

            @Override
            public void hasPrivilegesToFollowIndices(
                ThreadContext threadContext,
                RemoteClusterClient remoteClient,
                String[] indices,
                Consumer<Exception> handler
            ) {
                fail("Test case should fail before this code is called");
            }
        };

        String clusterAlias = randomIdentifier();

        ExecutorService mockExecutor = mock(ExecutorService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(eq(Ccr.CCR_THREAD_POOL_NAME))).thenReturn(mockExecutor);
        RemoteClusterClient mockRemoteClient = mock(RemoteClusterClient.class);

        Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        when(mockClient.getRemoteClusterClient(eq(clusterAlias), eq(mockExecutor), any())).thenReturn(mockRemoteClient);

        // When following an index that does not exist, throw IndexNotFoundException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, "non-existent-index");
            assertThat(exception, instanceOf(IndexNotFoundException.class));
            assertThat(exception.getMessage(), equalTo("no such index [non-existent-index]"));
        }

        // When following an alias, throw IllegalArgumentException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, aliasName);
            assertThat(exception, instanceOf(IllegalArgumentException.class));
            assertThat(exception.getMessage(), equalTo("cannot follow [" + aliasName + "], because it is a ALIAS"));
        }

        // When following a data stream, throw IllegalArgumentException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, dataStreamName);
            assertThat(exception, instanceOf(IllegalArgumentException.class));
            assertThat(exception.getMessage(), equalTo("cannot follow [" + dataStreamName + "], because it is a DATA_STREAM"));
        }

        // When following a data stream alias, throw IllegalArgumentException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, dsAliasName);
            assertThat(exception, instanceOf(IllegalArgumentException.class));
            assertThat(exception.getMessage(), equalTo("cannot follow [" + dsAliasName + "], because it is a ALIAS"));
        }

        // When following a closed index, throw IndexClosedException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, closedIndexName);
            assertThat(exception, instanceOf(IndexClosedException.class));
            assertThat(exception.getMessage(), equalTo("closed"));
        }

        // When following a failure store index, throw IllegalArgumentException
        {
            Exception exception = executeExpectingException(checker, mockClient, clusterAlias, firstFailureStore.getIndex().getName());
            assertThat(exception, instanceOf(IllegalArgumentException.class));
            assertThat(
                exception.getMessage(),
                equalTo("cannot follow [" + firstFailureStore.getIndex().getName() + "], because it is a failure store index")
            );
        }
    }

    private static Exception executeExpectingException(
        CcrLicenseChecker checker,
        Client mockClient,
        String clusterAlias,
        String leaderIndex
    ) {
        @SuppressWarnings("unchecked")
        Consumer<Exception> onFailure = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<String[], Tuple<IndexMetadata, DataStream>> consumer = mock(BiConsumer.class);
        checker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            mockClient,
            clusterAlias,
            leaderIndex,
            onFailure,
            consumer
        );
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(onFailure, times(1)).accept(captor.capture());
        verifyNoInteractions(consumer);
        return captor.getValue();
    }

}
