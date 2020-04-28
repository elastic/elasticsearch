/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.Objects;

public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutFollowAction.class);

    private final Client client;
    private final RestoreService restoreService;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportPutFollowAction(
            final ThreadPool threadPool,
            final TransportService transportService,
            final ClusterService clusterService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Client client,
            final RestoreService restoreService,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(
                PutFollowAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                PutFollowAction.Request::new,
                indexNameExpressionResolver);
        this.client = client;
        this.restoreService = restoreService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFollowAction.Response read(StreamInput in) throws IOException {
        return new PutFollowAction.Response(in);
    }

    @Override
    protected void masterOperation(
        Task task, final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        String remoteCluster = request.getRemoteCluster();
        // Validates whether the leader cluster has been configured properly:
        client.getRemoteClusterClient(remoteCluster);

        String leaderIndex = request.getLeaderIndex();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            listener::onFailure,
            (historyUUID, leaderIndexMetadata) -> createFollowerIndex(leaderIndexMetadata, request, listener));
    }

    private void createFollowerIndex(
            final IndexMetadata leaderIndexMetadata,
            final PutFollowAction.Request request,
            final ActionListener<PutFollowAction.Response> listener) {
        if (leaderIndexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetadata.getSettings()) == false) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() +
                "] does not have soft deletes enabled"));
            return;
        }

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getFollowerIndex())
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
        final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(request.getLeaderIndex()).indicesOptions(request.indicesOptions()).renamePattern("^(.*)$")
            .renameReplacement(request.getFollowerIndex()).masterNodeTimeout(request.masterNodeTimeout())
            .indexSettings(settingsBuilder);

        final Client clientWithHeaders = CcrLicenseChecker.wrapClient(this.client, threadPool.getThreadContext().getHeaders());
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                restoreService.restoreSnapshot(restoreRequest,
                    ActionListener.delegateFailure(listener,
                        (delegatedListener, response) -> afterRestoreStarted(clientWithHeaders, request, delegatedListener, response)));
            }
        });
    }

    private void afterRestoreStarted(Client clientWithHeaders, PutFollowAction.Request request,
                                     ActionListener<PutFollowAction.Response> originalListener,
                                     RestoreService.RestoreCompletionResponse response) {
        final ActionListener<PutFollowAction.Response> listener;
        if (ActiveShardCount.NONE.equals(request.waitForActiveShards())) {
            originalListener.onResponse(new PutFollowAction.Response(true, false, false));
            listener = new ActionListener<PutFollowAction.Response>() {

                @Override
                public void onResponse(PutFollowAction.Response response) {
                    logger.debug("put follow {} completed with {}", request, response);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> new ParameterizedMessage("put follow {} failed during the restore process", request), e);
                }
            };
        } else {
            listener = originalListener;
        }

        RestoreClusterStateListener.createAndRegisterListener(clusterService, response,
            ActionListener.delegateFailure(listener, (delegatedListener, restoreSnapshotResponse) -> {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo == null) {
                    // If restoreInfo is null then it is possible there was a master failure during the
                    // restore.
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                } else if (restoreInfo.failedShards() == 0) {
                    initiateFollowing(clientWithHeaders, request, delegatedListener);
                } else {
                    assert restoreInfo.failedShards() > 0 : "Should have failed shards";
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }));
    }

    private void initiateFollowing(
        final Client client,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "PutFollowAction does not support DEFAULT.";
        FollowParameters parameters = request.getParameters();
        ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request();
        resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
        resumeFollowRequest.setParameters(new FollowParameters(parameters));
        client.execute(ResumeFollowAction.INSTANCE, resumeFollowRequest, ActionListener.wrap(
            r -> activeShardsObserver.waitForActiveShards(new String[]{request.getFollowerIndex()},
                request.waitForActiveShards(), request.timeout(), result ->
                    listener.onResponse(new PutFollowAction.Response(true, result, r.isAcknowledged())),
                listener::onFailure),
            listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowerIndex());
    }
}
