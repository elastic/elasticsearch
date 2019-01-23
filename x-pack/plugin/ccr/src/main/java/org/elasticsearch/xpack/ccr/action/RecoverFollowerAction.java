/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

public class RecoverFollowerAction extends Action<RecoverFollowerAction.RecoverFollowerResponse> {

    public static final RecoverFollowerAction INSTANCE = new RecoverFollowerAction();
    public static final String NAME = "internal:admin/ccr/recover_follower";

    private RecoverFollowerAction() {
        super(NAME);
    }

    @Override
    public RecoverFollowerResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    public static class TransportRecoverFollowerAction extends TransportMasterNodeAction<RecoverFollowerRequest, RecoverFollowerResponse> {

        private final Client client;
        private final RestoreService restoreService;

        @Inject
        public TransportRecoverFollowerAction(TransportService transportService, ClusterService clusterService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              Client client, RestoreService restoreService) {
            super(NAME, true, transportService, clusterService, transportService.getThreadPool(), actionFilters,
                RecoverFollowerRequest::new, indexNameExpressionResolver);
            this.client = client;
            this.restoreService = restoreService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected RecoverFollowerResponse newResponse() {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        protected void masterOperation(RecoverFollowerRequest request, ClusterState state,
                                       ActionListener<RecoverFollowerResponse> listener) {
            PauseFollowAction.Request pauseRequest = new PauseFollowAction.Request(request.getFollowerIndex());

            // TODO: What to do if failed?

            client.execute(PauseFollowAction.INSTANCE, pauseRequest, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }

                            @Override
                            protected void doRun() throws Exception {
                                doRestore(request, listener);
                            }
                        });
                    } else {
                        listener.onFailure(new ElasticsearchException("Not acknowledged"));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(RecoverFollowerRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowerIndex());
        }

        private void doRestore(RecoverFollowerRequest request, ActionListener<RecoverFollowerResponse> listener) {
            String repoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
            Settings.Builder settingsBuilder = Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.getFollowerIndex())
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);

            RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(repoName, CcrRepository.LATEST)
                .indices(request.getLeaderIndex()).renamePattern("^(.*)$")
                .renameReplacement(request.getFollowerIndex()).masterNodeTimeout(request.masterNodeTimeout())
                .restoreOpenIndex(true).indexSettings(settingsBuilder.build());


            ActionListener<RestoreSnapshotResponse> restoreComplete = new ActionListener<RestoreSnapshotResponse>() {
                @Override
                public void onResponse(RestoreSnapshotResponse response) {
                    RestoreInfo restoreInfo = response.getRestoreInfo();
                    if (restoreInfo == null) {
                        // If restoreInfo is null then it is possible there was a master failure during the
                        // restore.
                        listener.onFailure(new ElasticsearchException("apparent master failure during restore"));
                    } else if (restoreInfo.failedShards() > 0) {
                        listener.onFailure(new ElasticsearchException("failed to restore [" + restoreInfo.failedShards()
                            + "] shards"));
                    } else {
                        // TODO: need to retain original follow request stuff
                        ResumeFollowAction.Request followRequest = new ResumeFollowAction.Request();
                        followRequest.setFollowerIndex(request.getFollowerIndex());
                        client.execute(ResumeFollowAction.INSTANCE, followRequest, new ActionListener<AcknowledgedResponse>() {
                            @Override
                            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                listener.onResponse(new RecoverFollowerResponse());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                        listener.onResponse(new RecoverFollowerResponse());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };

            restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreService.RestoreCompletionResponse>() {
                @Override
                public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                    RestoreClusterStateListener.createAndRegisterListener(clusterService, restoreCompletionResponse, restoreComplete);
                }

                @Override
                public void onFailure(Exception e) {
                    restoreComplete.onFailure(e);
                }
            });
        }
    }

    static class RecoverFollowerResponse extends ActionResponse {

    }
}
