/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

public class ResolveClusterAction extends ActionType<ResolveClusterAction.Response> {

    public static final ResolveClusterAction INSTANCE = new ResolveClusterAction();
    public static final String NAME = "indices:admin/resolve/cluster";

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        // we only allow querying against open, non-hidden indices
        public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

        private String[] names;
        private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

        public Request(String[] names) {
            this.names = names;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolveClusterAction.Request request = (ResolveClusterAction.Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        public boolean includeDataStreams() {
            // request must allow data streams because the index name expression resolver for the action handler assumes it
            return true;
        }
    }

    private ResolveClusterAction() {
        super(NAME, ResolveClusterAction.Response::new);
    }

    public static class ResolveClusterInfo implements Writeable {

        private final boolean connected;
        private final Boolean skipUnavailable;  // null for the local cluster
        private final Boolean matchingIndices;  // null means 'unknown' since we are not connected
        private final Build build;
        private final String error;

        public ResolveClusterInfo(boolean connected, Boolean skipUnavailable) {
            this(connected, skipUnavailable, null, null, null);
        }

        public ResolveClusterInfo(boolean connected, Boolean skipUnavailable, String error) {
            this(connected, skipUnavailable, null, null, error);
        }

        public ResolveClusterInfo(boolean connected, Boolean skipUnavailable, Boolean matchingIndices, Build build) {
            this(connected, skipUnavailable, matchingIndices, build, null);
        }

        private ResolveClusterInfo(boolean connected, Boolean skipUnavailable, Boolean matchingIndices, Build build, String error) {
            this.connected = connected;
            this.skipUnavailable = skipUnavailable;
            this.matchingIndices = matchingIndices;
            this.build = build;
            this.error = error;
            assert error != null || matchingIndices != null || connected == false : "If matchingIndices is null, connected must be false";
        }

        public ResolveClusterInfo(StreamInput in) throws IOException {
            this.connected = in.readBoolean();
            this.skipUnavailable = in.readOptionalBoolean();
            this.matchingIndices = in.readOptionalBoolean();
            this.error = in.readOptionalString();
            if (error == null) {
                this.build = Build.readBuild(in);
            } else {
                this.build = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(connected);
            out.writeOptionalBoolean(skipUnavailable);
            out.writeOptionalBoolean(matchingIndices);
            out.writeOptionalString(error);
            if (build != null) {
                Build.writeBuild(build, out);
            }
        }

        public boolean isConnected() {
            return connected;
        }

        public Boolean getSkipUnavailable() {
            return skipUnavailable;
        }

        public Boolean getMatchingIndices() {
            return matchingIndices;
        }

        public Build getBuild() {
            return build;
        }

        public String getError() {
            return error;
        }

        @Override
        public String toString() {
            return "ResolveClusterInfo{"
                + "connected="
                + connected
                + ", skipUnavailable="
                + skipUnavailable
                + ", matchingIndices="
                + matchingIndices
                + ", build="
                + build
                + ", error="
                + error
                + '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ParseField CONNECTED_FIELD = new ParseField("connected");
        private static final ParseField SKIP_UNAVAILABLE_FIELD = new ParseField("skip_unavailable");
        private static final ParseField MATCHING_INDICES_FIELD = new ParseField("matching_indices");
        private static final ParseField ES_VERSION_FIELD = new ParseField("version");
        private static final ParseField ERROR_FIELD = new ParseField("error");

        private final Map<String, ResolveClusterInfo> infoMap;

        public Response(Map<String, ResolveClusterInfo> infoMap) {
            this.infoMap = infoMap;
        }

        public Response(StreamInput in) throws IOException {
            this.infoMap = in.readImmutableMap(ResolveClusterAction.ResolveClusterInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(infoMap, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Map.Entry<String, ResolveClusterInfo> entry : infoMap.entrySet()) {
                String clusterAlias = entry.getKey();
                if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                    clusterAlias = "(local)";
                }
                builder.startObject(clusterAlias);
                ResolveClusterInfo clusterInfo = entry.getValue();
                builder.field(CONNECTED_FIELD.getPreferredName(), clusterInfo.isConnected());
                builder.field(SKIP_UNAVAILABLE_FIELD.getPreferredName(), clusterInfo.getSkipUnavailable());
                if (clusterInfo.getError() != null) {
                    builder.field(ERROR_FIELD.getPreferredName(), clusterInfo.getError());
                }
                if (clusterInfo.getMatchingIndices() != null) {
                    builder.field(MATCHING_INDICES_FIELD.getPreferredName(), clusterInfo.getMatchingIndices());
                }
                Build build = clusterInfo.getBuild();
                if (build != null) {
                    // TODO Lucene version is not part of build - do we want that as well?
                    // TODO should we create a new ClusterVersionInfo object that has Build, Lucene version and Features list?
                    builder.startObject(ES_VERSION_FIELD.getPreferredName())
                        .field("number", build.qualifiedVersion())
                        .field("build_flavor", build.flavor())
                        .field("minimum_wire_compatibility_version", build.minWireCompatVersion())
                        .field("minimum_index_compatibility_version", build.minIndexCompatVersion())
                        .endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolveClusterAction.Response response = (ResolveClusterAction.Response) o;
            return infoMap.equals(response.infoMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(infoMap);
        }

        public Map<String, ResolveClusterInfo> getResolveClusterInfo() {
            return infoMap;
        }
    }

    public static class TransportAction extends HandledTransportAction<ResolveClusterAction.Request, ResolveClusterAction.Response> {

        private final ThreadPool threadPool;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final boolean ccsCheckCompatibility;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(NAME, transportService, actionFilters, ResolveClusterAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.threadPool = threadPool;
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
        }

        @Override
        protected void doExecute(Task task, ResolveClusterAction.Request request, ActionListener<ResolveClusterAction.Response> listener) {
            if (ccsCheckCompatibility) {
                checkCCSVersionCompatibility(request);
            }
            ClusterState clusterState = clusterService.state();
            Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
                request.indicesOptions(),
                request.indices()
            );
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

            Map<String, ResolveClusterInfo> clusterInfoMap = new ConcurrentHashMap<>();
            if (remoteClusterIndices.size() > 0) {
                int remoteRequests = remoteClusterIndices.size();
                CountDown completionCounter = new CountDown(remoteRequests);
                Runnable terminalHandler = () -> {
                    if (completionCounter.countDown()) {
                        listener.onResponse(new ResolveClusterAction.Response(clusterInfoMap));
                    }
                };

                // add local cluster info if in scope of the index-expression from user
                if (localIndices != null) {
                    clusterInfoMap.put(
                        RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                        new ResolveClusterInfo(true, false, hasMatchingIndices(localIndices, clusterState), Build.current())
                    );
                }

                // make the cross-cluster calls
                for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                    String clusterAlias = remoteIndices.getKey();
                    OriginalIndices originalIndices = remoteIndices.getValue();
                    boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
                    Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                        threadPool,
                        clusterAlias,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    );
                    ResolveClusterAction.Request remoteRequest = new ResolveClusterAction.Request(originalIndices.indices());
                    remoteClusterClient.admin().indices().resolveCluster(remoteRequest, ActionListener.wrap(response -> {
                        ResolveClusterInfo info = response.getResolveClusterInfo().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        clusterInfoMap.put(
                            clusterAlias,
                            new ResolveClusterInfo(info.isConnected(), skipUnavailable, info.getMatchingIndices(), info.getBuild())
                        );
                        terminalHandler.run();
                    }, failure -> {
                        if (notConnectedError(failure)) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable));
                        } else {
                            Throwable cause = ExceptionsHelper.unwrapCause(failure);
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable, cause.toString()));
                            logger.warn(
                                () -> Strings.format("Failure from _resolve/cluster lookup against cluster %s: ", clusterAlias),
                                failure
                            );
                        }
                        terminalHandler.run();
                    }));
                }
            } else {
                clusterInfoMap.put(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    new ResolveClusterInfo(true, null, hasMatchingIndices(localIndices, clusterState), Build.current())
                );
                listener.onResponse(new ResolveClusterAction.Response(clusterInfoMap));
            }
        }

        private boolean notConnectedError(Exception e) {
            return e instanceof ConnectTransportException || e instanceof NoSuchRemoteClusterException;
        }

        private boolean hasMatchingIndices(OriginalIndices localIndices, ClusterState clusterState) {
            List<ResolveIndexAction.ResolvedIndex> indices = new ArrayList<>();
            List<ResolveIndexAction.ResolvedAlias> aliases = new ArrayList<>();           /// MP TODO: need this?
            List<ResolveIndexAction.ResolvedDataStream> dataStreams = new ArrayList<>();  /// MP TODO: need this?
            try {
                ResolveIndexAction.TransportAction.resolveIndices(
                    localIndices,
                    clusterState,
                    indexNameExpressionResolver,
                    indices,
                    aliases,
                    dataStreams
                );

            } catch (IndexNotFoundException e) {
                return false;
            }

            // TODO: not sure that this is right - does aliases.size > 0 imply that there are indices in scope for the search?
            return indices.size() > 0 || aliases.size() > 0 || dataStreams.size() > 0;
        }
    }
}
