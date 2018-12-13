/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeprecationInfoAction extends Action<DeprecationInfoAction.Response> {

    public static final DeprecationInfoAction INSTANCE = new DeprecationInfoAction();
    public static final String NAME = "cluster:admin/xpack/deprecation/info";

    private DeprecationInfoAction() {
        super(NAME);
    }

    /**
     * helper utility function to reduce repeat of running a specific {@link Set} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (BiFunction, Function, including the appropriate arguments)
     * @return The list of {@link DeprecationIssue} that were found in the cluster
     */
    public static <T> List<DeprecationIssue> filterChecks(List<T> checks, Function<T, DeprecationIssue> mapper) {
        return checks.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private List<DeprecationIssue> clusterSettingsIssues;
        private List<DeprecationIssue> nodeSettingsIssues;
        private Map<String, List<DeprecationIssue>> indexSettingsIssues;

        public Response() {
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
        public static DeprecationInfoAction.Response from(List<NodeInfo> nodesInfo, List<NodeStats> nodesStats, ClusterState state,
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

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
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
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
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

}
