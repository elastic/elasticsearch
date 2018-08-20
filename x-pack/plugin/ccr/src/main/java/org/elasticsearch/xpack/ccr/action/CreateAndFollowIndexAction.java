/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateAndFollowIndexAction extends Action<CreateAndFollowIndexAction.Response> {

    public static final CreateAndFollowIndexAction INSTANCE = new CreateAndFollowIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/create_and_follow_index";

    private CreateAndFollowIndexAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private FollowIndexAction.Request followRequest;

        public Request(FollowIndexAction.Request followRequest) {
            this.followRequest = Objects.requireNonNull(followRequest);
        }

        Request() {
        }

        public FollowIndexAction.Request getFollowRequest() {
            return followRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return followRequest.validate();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            followRequest = new FollowIndexAction.Request();
            followRequest.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            followRequest.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(followRequest, request.followRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(followRequest);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean followIndexCreated;
        private boolean followIndexShardsAcked;
        private boolean indexFollowingStarted;

        Response() {
        }

        Response(boolean followIndexCreated, boolean followIndexShardsAcked, boolean indexFollowingStarted) {
            this.followIndexCreated = followIndexCreated;
            this.followIndexShardsAcked = followIndexShardsAcked;
            this.indexFollowingStarted = indexFollowingStarted;
        }

        public boolean isFollowIndexCreated() {
            return followIndexCreated;
        }

        public boolean isFollowIndexShardsAcked() {
            return followIndexShardsAcked;
        }

        public boolean isIndexFollowingStarted() {
            return indexFollowingStarted;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            followIndexCreated = in.readBoolean();
            followIndexShardsAcked = in.readBoolean();
            indexFollowingStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(followIndexCreated);
            out.writeBoolean(followIndexShardsAcked);
            out.writeBoolean(indexFollowingStarted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("follow_index_created", followIndexCreated);
                builder.field("follow_index_shards_acked", followIndexShardsAcked);
                builder.field("index_following_started", indexFollowingStarted);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return followIndexCreated == response.followIndexCreated &&
                followIndexShardsAcked == response.followIndexShardsAcked &&
                indexFollowingStarted == response.indexFollowingStarted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final Client client;
        private final AllocationService allocationService;
        private final RemoteClusterService remoteClusterService;
        private final ActiveShardsObserver activeShardsObserver;
        private final CcrLicenseChecker ccrLicenseChecker;

        @Inject
        public TransportAction(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ClusterService clusterService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final Client client,
                final AllocationService allocationService,
                final CcrLicenseChecker ccrLicenseChecker) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.allocationService = allocationService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
            this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(
                final Request request, final ClusterState state, final ActionListener<Response> listener) throws Exception {
            if (ccrLicenseChecker.isCcrAllowed()) {
                final String[] indices = new String[]{request.getFollowRequest().getLeaderIndex()};
                final Map<String, List<String>> remoteClusterIndices = remoteClusterService.groupClusterIndices(indices, s -> false);
                if (remoteClusterIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                    createFollowerIndexAndFollowLocalIndex(request, state, listener);
                } else {
                    assert remoteClusterIndices.size() == 1;
                    final Map.Entry<String, List<String>> entry = remoteClusterIndices.entrySet().iterator().next();
                    assert entry.getValue().size() == 1;
                    final String clusterAlias = entry.getKey();
                    final String leaderIndex = entry.getValue().get(0);
                    createFollowerIndexAndFollowRemoteIndex(request, clusterAlias, leaderIndex, listener);
                }
            } else {
                listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            }
        }

        private void createFollowerIndexAndFollowLocalIndex(
                final Request request, final ClusterState state, final ActionListener<Response> listener) {
            // following an index in local cluster, so use local cluster state to fetch leader index metadata
            final IndexMetaData leaderIndexMetadata = state.getMetaData().index(request.getFollowRequest().getLeaderIndex());
            createFollowerIndex(leaderIndexMetadata, request, listener);
        }

        private void createFollowerIndexAndFollowRemoteIndex(
                final Request request,
                final String clusterAlias,
                final String leaderIndex,
                final ActionListener<Response> listener) {
            ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadata(
                    client,
                    clusterAlias,
                    leaderIndex,
                    listener,
                    leaderIndexMetaData -> createFollowerIndex(leaderIndexMetaData, request, listener));
        }

        private void createFollowerIndex(
                final IndexMetaData leaderIndexMetaData, final Request request, final ActionListener<Response> listener) {
            if (leaderIndexMetaData == null) {
                listener.onFailure(new IllegalArgumentException("leader index [" + request.getFollowRequest().getLeaderIndex() +
                    "] does not exist"));
                return;
            }

            ActionListener<Boolean> handler = ActionListener.wrap(
                result -> {
                    if (result) {
                        initiateFollowing(request, listener);
                    } else {
                        listener.onResponse(new Response(true, false, false));
                    }
                },
                listener::onFailure);
            // Can't use create index api here, because then index templates can alter the mappings / settings.
            // And index templates could introduce settings / mappings that are incompatible with the leader index.
            clusterService.submitStateUpdateTask("follow_index_action", new AckedClusterStateUpdateTask<Boolean>(request, handler) {

                @Override
                protected Boolean newResponse(boolean acknowledged) {
                    return acknowledged;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    String followIndex = request.getFollowRequest().getFollowerIndex();
                    IndexMetaData currentIndex = currentState.metaData().index(followIndex);
                    if (currentIndex != null) {
                        throw new ResourceAlreadyExistsException(currentIndex.getIndex());
                    }

                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    IndexMetaData.Builder imdBuilder = IndexMetaData.builder(followIndex);

                    // Copy all settings, but overwrite a few settings.
                    Settings.Builder settingsBuilder = Settings.builder();
                    settingsBuilder.put(leaderIndexMetaData.getSettings());
                    // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
                    settingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                    settingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followIndex);
                    settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
                    imdBuilder.settings(settingsBuilder);

                    // Copy mappings from leader IMD to follow IMD
                    for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
                        imdBuilder.putMapping(cursor.value);
                    }
                    imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());
                    IndexMetaData followIMD = imdBuilder.build();
                    mdBuilder.put(followIMD, false);

                    ClusterState.Builder builder = ClusterState.builder(currentState);
                    builder.metaData(mdBuilder.build());
                    ClusterState updatedState = builder.build();

                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                        .addAsNew(updatedState.metaData().index(request.getFollowRequest().getFollowerIndex()));
                    updatedState = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                        "follow index [" + request.getFollowRequest().getFollowerIndex() + "] created");

                    logger.info("[{}] creating index, cause [ccr_create_and_follow], shards [{}]/[{}]",
                        followIndex, followIMD.getNumberOfShards(), followIMD.getNumberOfReplicas());

                    return updatedState;
                }
            });
        }

        private void initiateFollowing(Request request, ActionListener<Response> listener) {
            activeShardsObserver.waitForActiveShards(new String[]{request.followRequest.getFollowerIndex()},
                ActiveShardCount.DEFAULT, request.timeout(), result -> {
                    if (result) {
                        client.execute(FollowIndexAction.INSTANCE, request.getFollowRequest(), ActionListener.wrap(
                            r -> listener.onResponse(new Response(true, true, r.isAcknowledged())),
                            listener::onFailure
                        ));
                    } else {
                        listener.onResponse(new Response(true, false, false));
                    }
                }, listener::onFailure);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowRequest().getFollowerIndex());
        }

    }

}
