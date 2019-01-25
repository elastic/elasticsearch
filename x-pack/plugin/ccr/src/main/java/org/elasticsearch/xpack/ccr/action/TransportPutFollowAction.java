/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.Objects;

public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private static final ActionListener<PutFollowAction.Response> NOOP_LISTENER = ActionListener.wrap(() -> {});

    private final Client client;
    private final RestoreService restoreService;
    private final CcrLicenseChecker ccrLicenseChecker;

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
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFollowAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected PutFollowAction.Response read(StreamInput in) throws IOException {
        return new PutFollowAction.Response(in);
    }

    @Override
    protected void masterOperation(
            final PutFollowAction.Request request,
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
            (historyUUID, leaderIndexMetaData) -> createFollowerIndex(leaderIndexMetaData, request, listener));
    }

    private void createFollowerIndex(
            final IndexMetaData leaderIndexMetaData,
            final PutFollowAction.Request request,
            final ActionListener<PutFollowAction.Response> listener) {
        if (leaderIndexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        // soft deletes are enabled by default on indices created on 7.0.0 or later
        if (leaderIndexMetaData.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
            IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(leaderIndexMetaData.getSettings()).onOrAfter(Version.V_7_0_0)) == false) {
            listener.onFailure(
                new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled"));
            return;
        }

        final ActionListener<PutFollowAction.Response> followingListener;
        final ActionListener<PutFollowAction.Response> restoreInitiatedListener;
        if (request.getWaitForRestore()) {
            followingListener = listener;
            restoreInitiatedListener = NOOP_LISTENER;
        } else {
            followingListener = NOOP_LISTENER;
            restoreInitiatedListener = listener;
        }

        Client clientWithHeaders = CcrLicenseChecker.wrapClient(this.client, threadPool.getThreadContext().getHeaders());

        ActionListener<RestoreSnapshotResponse> restoreCompleteHandler = new ActionListener<RestoreSnapshotResponse>() {
            @Override
            public void onResponse(RestoreSnapshotResponse restoreSnapshotResponse) {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();

                if (restoreInfo == null) {
                    // If restoreInfo is null then it is possible there was a master failure during the
                    // restore.
                    followingListener.onResponse(new PutFollowAction.Response(true, false, false));
                } else if (restoreInfo.failedShards() == 0) {
                    initiateFollowing(clientWithHeaders, request, followingListener);
                } else {
                    // Has failed shards
                    followingListener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }

            @Override
            public void onFailure(Exception e) {
                followingListener.onFailure(e);
            }
        };

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.getFollowRequest().getFollowerIndex())
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
        final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(request.getLeaderIndex()).indicesOptions(request.indicesOptions()).renamePattern("^(.*)$")
            .renameReplacement(request.getFollowRequest().getFollowerIndex()).masterNodeTimeout(request.masterNodeTimeout())
            .indexSettings(settingsBuilder);
        initiateRestore(restoreRequest, restoreInitiatedListener, restoreCompleteHandler);
    }

    private void initiateRestore(RestoreSnapshotRequest restoreRequest, ActionListener<PutFollowAction.Response> restoreInitiatedListener,
                                 ActionListener<RestoreSnapshotResponse> restoreCompleteListener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                restoreInitiatedListener.onFailure(e);
                restoreCompleteListener.onFailure(e);
            }

            @Override
            protected void doRun() {
                restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreService.RestoreCompletionResponse>() {
                    @Override
                    public void onResponse(RestoreService.RestoreCompletionResponse response) {
                        restoreInitiatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                        RestoreClusterStateListener.createAndRegisterListener(clusterService, response, restoreCompleteListener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restoreInitiatedListener.onFailure(e);
                        restoreCompleteListener.onFailure(e);
                    }
                });
            }
        });

    }

    private void initiateFollowing(
        final Client client,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        client.execute(ResumeFollowAction.INSTANCE, request.getFollowRequest(), ActionListener.wrap(
            r -> listener.onResponse(new PutFollowAction.Response(true, true, r.isAcknowledged())),
            listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowRequest().getFollowerIndex());
    }
}
