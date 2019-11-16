/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeprecationInfoAction extends ActionType<DeprecationInfoAction.Response> {

    public static final DeprecationInfoAction INSTANCE = new DeprecationInfoAction();
    public static final String NAME = "cluster:admin/xpack/deprecation/info";

    private DeprecationInfoAction() {
        super(NAME, DeprecationInfoAction.Response::new);
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

    public static class Response extends ActionResponse implements ToXContentObject {
        private List<DeprecationIssue> clusterSettingsIssues;
        private List<DeprecationIssue> nodeSettingsIssues;
        private Map<String, List<DeprecationIssue>> indexSettingsIssues;
        private List<DeprecationIssue> mlSettingsIssues;

        public Response(StreamInput in) throws IOException {
            super(in);
            clusterSettingsIssues = in.readList(DeprecationIssue::new);
            nodeSettingsIssues = in.readList(DeprecationIssue::new);
            indexSettingsIssues = in.readMapOfLists(StreamInput::readString, DeprecationIssue::new);
            mlSettingsIssues = in.readList(DeprecationIssue::new);
        }

        public Response(List<DeprecationIssue> clusterSettingsIssues,
                        List<DeprecationIssue> nodeSettingsIssues,
                        Map<String, List<DeprecationIssue>> indexSettingsIssues,
                        List<DeprecationIssue> mlSettingsIssues) {
            this.clusterSettingsIssues = clusterSettingsIssues;
            this.nodeSettingsIssues = nodeSettingsIssues;
            this.indexSettingsIssues = indexSettingsIssues;
            this.mlSettingsIssues = mlSettingsIssues;
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

        public List<DeprecationIssue> getMlSettingsIssues() {
            return mlSettingsIssues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(clusterSettingsIssues);
            out.writeList(nodeSettingsIssues);
            out.writeMapOfLists(indexSettingsIssues, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            out.writeList(mlSettingsIssues);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .array("cluster_settings", clusterSettingsIssues.toArray())
                .array("node_settings", nodeSettingsIssues.toArray())
                .field("index_settings")
                .map(indexSettingsIssues)
                .array("ml_settings", mlSettingsIssues.toArray())
                .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(clusterSettingsIssues, response.clusterSettingsIssues) &&
                Objects.equals(nodeSettingsIssues, response.nodeSettingsIssues) &&
                Objects.equals(indexSettingsIssues, response.indexSettingsIssues) &&
                Objects.equals(mlSettingsIssues, response.mlSettingsIssues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues, mlSettingsIssues);
        }

        /**
         * This is the function that does the bulk of the logic of taking the appropriate ES dependencies
         * like {@link NodeInfo}, {@link ClusterState}. Alongside these objects and the list of deprecation checks,
         * this function will run through all the checks and build out the final list of issues that exist in the
         * cluster.
         *
         * @param state The cluster state
         * @param indexNameExpressionResolver Used to resolve indices into their concrete names
         * @param indices The list of index expressions to evaluate using `indexNameExpressionResolver`
         * @param indicesOptions The options to use when resolving and filtering which indices to check
         * @param datafeeds The ml datafeed configurations, if ML is enabled
         * @param rolesResponse The configured roles, if security is enabled
         * @param fileRolesResponse The configured file-based roles, for each node, if security is enabled
         * @param indexSettingsChecks The list of index-level checks that will be run across all specified
         *                            concrete indices
         * @param clusterSettingsChecks The list of cluster-level checks
         * @param mlSettingsCheck The list of ml checks
         * @param rolesChecks The list of security checks
         * @param nodeDeprecationResponse The response containing the deprecation issues found on each node
         * @return The list of deprecation issues found in the cluster
         */
        public static DeprecationInfoAction.Response from(ClusterState state,
                                                          NamedXContentRegistry xContentRegistry,
                                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                                          String[] indices, IndicesOptions indicesOptions,
                                                          List<DatafeedConfig> datafeeds,
                                                          GetRolesResponse rolesResponse,
                                                          GetFileRolesResponse fileRolesResponse,
                                                          List<Function<IndexMetaData, DeprecationIssue>> indexSettingsChecks,
                                                          List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks,
                                                          List<BiFunction<DatafeedConfig, NamedXContentRegistry, DeprecationIssue>>
                                                              mlSettingsCheck,
                                                          List<Function<List<RoleDescriptor>, DeprecationIssue>> rolesChecks,
                                                          NodesDeprecationCheckResponse nodeDeprecationResponse) {
            List<DeprecationIssue> clusterSettingsIssues = filterChecks(clusterSettingsChecks,
                (c) -> c.apply(state));
            List<DeprecationIssue> mlSettingsIssues = new ArrayList<>();
            for (DatafeedConfig config : datafeeds) {
                mlSettingsIssues.addAll(filterChecks(mlSettingsCheck, (c) -> c.apply(config, xContentRegistry)));
            }

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

            // Security stuff
            final List<DeprecationIssue> rolesIssues = filterChecks(rolesChecks, c -> c.apply(Arrays.asList(rolesResponse.roles())));
            final List<DeprecationIssue> mergedClusterIssues = Stream.concat(clusterSettingsIssues.stream(), rolesIssues.stream())
                .collect(Collectors.toList());

            final Map<DiscoveryNode, List<DeprecationIssue>> rolesIssuesByNode = runNodeRoleChecks(fileRolesResponse, rolesChecks);
            final Map<DiscoveryNode, List<DeprecationIssue>> nodeIssues = nodeResponseToMap(nodeDeprecationResponse);
            final List<DeprecationIssue> mergedNodeIssues = mergeNodeIssues(nodeIssues, rolesIssuesByNode);

            return new DeprecationInfoAction.Response(mergedClusterIssues, mergedNodeIssues, indexSettingsIssues, mlSettingsIssues);
        }

        private static List<DeprecationIssue> mergeNodeIssues(Map<DiscoveryNode, List<DeprecationIssue>> nodeIssues,
                                                              Map<DiscoveryNode, List<DeprecationIssue>> rolesIssuesByNode) {
            Map<DeprecationIssue, List<String>> issuesToNodeMap = new HashMap<>();
            Stream.concat(nodeIssues.entrySet().stream(), rolesIssuesByNode.entrySet().stream())
                .forEach(entry -> {
                    String nodeName = entry.getKey().getName();
                    entry.getValue().stream()
                        .forEach(issue -> issuesToNodeMap.computeIfAbsent(issue, i -> new ArrayList<>()).add(nodeName));
                });
            return issuesToNodeMap.entrySet().stream()
                .map(entry -> {
                    DeprecationIssue issue = entry.getKey();
                    String details = issue.getDetails() != null ? issue.getDetails() + " " : "";
                    return new DeprecationIssue(issue.getLevel(), issue.getMessage(), issue.getUrl(),
                        details + "(nodes impacted: " + entry.getValue() + ")");
                }).collect(Collectors.toList());
        }

        private static Map<DiscoveryNode, List<DeprecationIssue>> runNodeRoleChecks(GetFileRolesResponse fileRolesResponse,
                                                                                    List<Function<List<RoleDescriptor>,
                                                                                        DeprecationIssue>> rolesChecks) {
            return fileRolesResponse.getNodes().stream()
                .collect(Collectors.groupingBy(BaseNodeResponse::getNode,
                    Collectors.flatMapping(nodeResp -> filterChecks(rolesChecks, (c) -> c.apply(nodeResp.getRoles())).stream(),
                        Collectors.toList())));
        }

        private static Map<DiscoveryNode, List<DeprecationIssue>> nodeResponseToMap(NodesDeprecationCheckResponse nodeDeprecationResponse) {
            return nodeDeprecationResponse.getNodes().stream()
                .collect(Collectors.groupingBy(BaseNodeResponse::getNode,
                    Collectors.flatMapping(nodeResp -> nodeResp.getDeprecationIssues().stream(),
                        Collectors.toList())));
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
