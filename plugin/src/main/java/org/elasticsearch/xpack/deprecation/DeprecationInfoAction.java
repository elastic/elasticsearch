/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.deprecation.DeprecationIssue.Level;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.filterChecks;

public class DeprecationInfoAction extends Action<DeprecationInfoAction.Request,
    DeprecationInfoAction.Response, DeprecationInfoAction.RequestBuilder> {

    public static final DeprecationInfoAction INSTANCE = new DeprecationInfoAction();
    public static final String NAME = "cluster:admin/xpack/deprecation/info";

    private DeprecationInfoAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private List<DeprecationIssue> clusterSettingsIssues;
        private List<DeprecationIssue> nodeSettingsIssues;
        private Map<String, List<DeprecationIssue>> indexSettingsIssues;

        Response() {
        }

        public Response(List<DeprecationIssue> clusterSettingsIssues,
                        List<DeprecationIssue> nodeSettingsIssues,
                        Map<String, List<DeprecationIssue>> indexSettingsIssues) {
            this.clusterSettingsIssues = clusterSettingsIssues;
            this.nodeSettingsIssues = nodeSettingsIssues;
            this.indexSettingsIssues = indexSettingsIssues;
        }

        public List<DeprecationIssue> getClusterSettingsIssues() {
            return clusterSettingsIssues;
        }

        public List<DeprecationIssue> getNodeSettingsIssues() {
            return nodeSettingsIssues;
        }

        public Map<String, List<DeprecationIssue>> getIndexSettingsIssues() {
            return indexSettingsIssues;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterSettingsIssues = in.readList(DeprecationIssue::new);
            nodeSettingsIssues = in.readList(DeprecationIssue::new);
            indexSettingsIssues = in.readMapOfLists(StreamInput::readString, DeprecationIssue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(clusterSettingsIssues);
            out.writeList(nodeSettingsIssues);
            out.writeMapOfLists(indexSettingsIssues, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .array("cluster_settings", clusterSettingsIssues.toArray())
                .array("node_settings", nodeSettingsIssues.toArray())
                .field("index_settings")
                .map(indexSettingsIssues)
                .endObject();
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(clusterSettingsIssues, response.clusterSettingsIssues) &&
                Objects.equals(nodeSettingsIssues, response.nodeSettingsIssues) &&
                Objects.equals(indexSettingsIssues, response.indexSettingsIssues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues);
        }

        /**
         * This is the function that does the bulk of the logic of taking the appropriate ES dependencies
         * like {@link NodeInfo}, {@link ClusterState}. Alongside these objects and the list of deprecation checks,
         * this function will run through all the checks and build out the final list of issues that exist in the
         * cluster.
         *
         * @param nodesInfo The list of {@link NodeInfo} metadata objects for retrieving node-level information
         * @param nodesStats The list of {@link NodeStats} metadata objects for retrieving node-level information
         * @param state The cluster state
         * @param indexNameExpressionResolver Used to resolve indices into their concrete names
         * @param indices The list of index expressions to evaluate using `indexNameExpressionResolver`
         * @param indicesOptions The options to use when resolving and filtering which indices to check
         * @param clusterSettingsChecks The list of cluster-level checks
         * @param nodeSettingsChecks The list of node-level checks
         * @param indexSettingsChecks The list of index-level checks that will be run across all specified
         *                            concrete indices
         * @return The list of deprecation issues found in the cluster
         */
        static DeprecationInfoAction.Response from(List<NodeInfo> nodesInfo, List<NodeStats> nodesStats, ClusterState state,
                                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                                   String[] indices, IndicesOptions indicesOptions,
                                                   List<Function<ClusterState,DeprecationIssue>>clusterSettingsChecks,
                                                   List<BiFunction<List<NodeInfo>, List<NodeStats>, DeprecationIssue>> nodeSettingsChecks,
                                                   List<Function<IndexMetaData, DeprecationIssue>> indexSettingsChecks) {
            List<DeprecationIssue> clusterSettingsIssues = filterChecks(clusterSettingsChecks,
                (c) -> c.apply(state));
            List<DeprecationIssue> nodeSettingsIssues = filterChecks(nodeSettingsChecks,
                (c) -> c.apply(nodesInfo, nodesStats));

            String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, indices);

            Map<String, List<DeprecationIssue>> indexSettingsIssues = new HashMap<>();
            for (String concreteIndex : concreteIndexNames) {
                IndexMetaData indexMetaData = state.getMetaData().index(concreteIndex);
                List<DeprecationIssue> singleIndexIssues = filterChecks(indexSettingsChecks,
                    c -> c.apply(indexMetaData));
                if (singleIndexIssues.size() > 0) {
                    indexSettingsIssues.put(concreteIndex, singleIndexIssues);
                }
            }

            return new DeprecationInfoAction.Response(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues);
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private String[] indices = Strings.EMPTY_ARRAY;
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, true,
            true, true);

        public Request() {
        }

        public Request(String... indices) {
            this.indices = indices;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (indices == null) {
                validationException = addValidationError("index/indices is missing", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices));
        }

    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, DeprecationInfoAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder setIndices(String... indices) {
            request.indices(indices);
            return this;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final XPackLicenseState licenseState;
        private final InternalClient client;
        private final IndexNameExpressionResolver indexNameExpressionResolver;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               XPackLicenseState licenseState, InternalClient client) {
            super(settings, DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, Request::new);
            this.licenseState = licenseState;
            this.client = client;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected final void masterOperation(final Request request, ClusterState state, final ActionListener<Response> listener) {
            if (licenseState.isDeprecationAllowed()) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest("_local").settings(true).plugins(true);
                NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("_local").fs(true);

                client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(
                    nodesInfoResponse -> {
                        if (nodesInfoResponse.hasFailures()) {
                            throw nodesInfoResponse.failures().get(0);
                        }
                        client.admin().cluster().nodesStats(nodesStatsRequest, ActionListener.wrap(
                            nodesStatsResponse -> {
                                if (nodesStatsResponse.hasFailures()) {
                                    throw nodesStatsResponse.failures().get(0);
                                }
                                listener.onResponse(Response.from(nodesInfoResponse.getNodes(),
                                    nodesStatsResponse.getNodes(), state, indexNameExpressionResolver,
                                    request.indices(), request.indicesOptions(), CLUSTER_SETTINGS_CHECKS,
                                    NODE_SETTINGS_CHECKS, INDEX_SETTINGS_CHECKS));
                            }, listener::onFailure));
                    },listener::onFailure));
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.DEPRECATION));
            }
        }
    }
}
