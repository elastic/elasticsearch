/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.retentionLeaseId;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CcrRepositoryRetentionLeaseTests extends ESTestCase {

    public void testWhenRetentionLeaseAlreadyExistsWeTryToRenewIt() {
        final RepositoryMetadata repositoryMetadata = mock(RepositoryMetadata.class);
        when(repositoryMetadata.name()).thenReturn(CcrRepository.NAME_PREFIX);
        final Set<Setting<?>> settings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            CcrSettings.getSettings().stream().filter(Setting::hasNodeScope)
        ).collect(Collectors.toSet());

        final CcrRepository repository = createCcrRepository(repositoryMetadata, settings);

        final ShardId followerShardId = new ShardId(new Index("follower-index-name", "follower-index-uuid"), 0);
        final ShardId leaderShardId = new ShardId(new Index("leader-index-name", "leader-index-uuid"), 0);

        final String retentionLeaseId = retentionLeaseId(
            "local-cluster",
            followerShardId.getIndex(),
            "remote-cluster",
            leaderShardId.getIndex()
        );

        // simulate that the retention lease already exists on the leader, and verify that we attempt to renew it
        final Client remoteClient = mock(Client.class);
        final ArgumentCaptor<RetentionLeaseActions.AddRequest> addRequestCaptor = ArgumentCaptor.forClass(
            RetentionLeaseActions.AddRequest.class
        );
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) invocationOnMock.getArguments()[2];
            listener.onFailure(new RetentionLeaseAlreadyExistsException(retentionLeaseId));
            return null;
        }).when(remoteClient).execute(same(RetentionLeaseActions.Add.INSTANCE), addRequestCaptor.capture(), any());
        final ArgumentCaptor<RetentionLeaseActions.RenewRequest> renewRequestCaptor = ArgumentCaptor.forClass(
            RetentionLeaseActions.RenewRequest.class
        );
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) invocationOnMock.getArguments()[2];
            listener.onResponse(ActionResponse.Empty.INSTANCE);
            return null;
        }).when(remoteClient).execute(same(RetentionLeaseActions.Renew.INSTANCE), renewRequestCaptor.capture(), any());

        repository.acquireRetentionLeaseOnLeader(followerShardId, retentionLeaseId, leaderShardId, remoteClient);

        verify(remoteClient).execute(same(RetentionLeaseActions.Add.INSTANCE), any(RetentionLeaseActions.AddRequest.class), any());
        assertThat(addRequestCaptor.getValue().getShardId(), equalTo(leaderShardId));
        assertThat(addRequestCaptor.getValue().getId(), equalTo(retentionLeaseId));
        assertThat(addRequestCaptor.getValue().getRetainingSequenceNumber(), equalTo(RETAIN_ALL));
        assertThat(addRequestCaptor.getValue().getSource(), equalTo("ccr"));

        verify(remoteClient).execute(same(RetentionLeaseActions.Renew.INSTANCE), any(RetentionLeaseActions.RenewRequest.class), any());
        assertThat(renewRequestCaptor.getValue().getShardId(), equalTo(leaderShardId));
        assertThat(renewRequestCaptor.getValue().getId(), equalTo(retentionLeaseId));
        assertThat(renewRequestCaptor.getValue().getRetainingSequenceNumber(), equalTo(RETAIN_ALL));
        assertThat(renewRequestCaptor.getValue().getSource(), equalTo("ccr"));

        verifyNoMoreInteractions(remoteClient);
    }

    private static CcrRepository createCcrRepository(RepositoryMetadata repositoryMetadata, Set<Setting<?>> settings) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        return new CcrRepository(
            repositoryMetadata,
            mock(Client.class),
            Settings.EMPTY,
            new CcrSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, settings)),
            threadPool
        );
    }

    public void testWhenRetentionLeaseExpiresBeforeWeCanRenewIt() {
        final RepositoryMetadata repositoryMetadata = mock(RepositoryMetadata.class);
        when(repositoryMetadata.name()).thenReturn(CcrRepository.NAME_PREFIX);
        final Set<Setting<?>> settings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            CcrSettings.getSettings().stream().filter(Setting::hasNodeScope)
        ).collect(Collectors.toSet());

        final CcrRepository repository = createCcrRepository(repositoryMetadata, settings);

        final ShardId followerShardId = new ShardId(new Index("follower-index-name", "follower-index-uuid"), 0);
        final ShardId leaderShardId = new ShardId(new Index("leader-index-name", "leader-index-uuid"), 0);

        final String retentionLeaseId = retentionLeaseId(
            "local-cluster",
            followerShardId.getIndex(),
            "remote-cluster",
            leaderShardId.getIndex()
        );

        // simulate that the retention lease already exists on the leader, expires before we renew, and verify that we attempt to add it
        final Client remoteClient = mock(Client.class);
        final ArgumentCaptor<RetentionLeaseActions.AddRequest> addRequestCaptor = ArgumentCaptor.forClass(
            RetentionLeaseActions.AddRequest.class
        );
        final PlainActionFuture<ActionResponse.Empty> response = new PlainActionFuture<>();
        response.onResponse(ActionResponse.Empty.INSTANCE);
        doAnswer(new Answer<Void>() {

            final AtomicBoolean firstInvocation = new AtomicBoolean(true);

            @Override
            public Void answer(final InvocationOnMock invocationOnMock) {
                @SuppressWarnings("unchecked")
                final ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) invocationOnMock
                    .getArguments()[2];
                if (firstInvocation.compareAndSet(true, false)) {
                    listener.onFailure(new RetentionLeaseAlreadyExistsException(retentionLeaseId));
                } else {
                    listener.onResponse(ActionResponse.Empty.INSTANCE);
                }
                return null;
            }

        }).when(remoteClient).execute(same(RetentionLeaseActions.Add.INSTANCE), addRequestCaptor.capture(), any());
        final ArgumentCaptor<RetentionLeaseActions.RenewRequest> renewRequestCaptor = ArgumentCaptor.forClass(
            RetentionLeaseActions.RenewRequest.class
        );
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) invocationOnMock.getArguments()[2];
            listener.onFailure(new RetentionLeaseNotFoundException(retentionLeaseId));
            return null;
        }).when(remoteClient).execute(same(RetentionLeaseActions.Renew.INSTANCE), renewRequestCaptor.capture(), any());

        repository.acquireRetentionLeaseOnLeader(followerShardId, retentionLeaseId, leaderShardId, remoteClient);

        verify(remoteClient, times(2)).execute(
            same(RetentionLeaseActions.Add.INSTANCE),
            any(RetentionLeaseActions.AddRequest.class),
            any()
        );
        assertThat(addRequestCaptor.getValue().getShardId(), equalTo(leaderShardId));
        assertThat(addRequestCaptor.getValue().getId(), equalTo(retentionLeaseId));
        assertThat(addRequestCaptor.getValue().getRetainingSequenceNumber(), equalTo(RETAIN_ALL));
        assertThat(addRequestCaptor.getValue().getSource(), equalTo("ccr"));

        verify(remoteClient).execute(same(RetentionLeaseActions.Renew.INSTANCE), any(RetentionLeaseActions.RenewRequest.class), any());
        assertThat(renewRequestCaptor.getValue().getShardId(), equalTo(leaderShardId));
        assertThat(renewRequestCaptor.getValue().getId(), equalTo(retentionLeaseId));
        assertThat(renewRequestCaptor.getValue().getRetainingSequenceNumber(), equalTo(RETAIN_ALL));
        assertThat(renewRequestCaptor.getValue().getSource(), equalTo("ccr"));

        verifyNoMoreInteractions(remoteClient);
    }

}
