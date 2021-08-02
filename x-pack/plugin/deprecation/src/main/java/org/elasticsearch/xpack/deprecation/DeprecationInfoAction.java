/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private static List<DeprecationIssue> mergeNodeIssues(NodesDeprecationCheckResponse response) {
        Map<DeprecationIssue, List<String>> issueListMap = new HashMap<>();
        for (NodesDeprecationCheckAction.NodeResponse resp : response.getNodes()) {
            for (DeprecationIssue issue : resp.getDeprecationIssues()) {
                issueListMap.computeIfAbsent(issue, (key) -> new ArrayList<>()).add(resp.getNode().getName());
            }
        }

        return issueListMap.entrySet().stream()
            .map(entry -> {
                DeprecationIssue issue = entry.getKey();
                String details = issue.getDetails() != null ? issue.getDetails() + " " : "";
                return new DeprecationIssue(issue.getLevel(), issue.getMessage(), issue.getUrl(),
                    details + "(nodes impacted: " + entry.getValue() + ")", issue.isResolveDuringRollingUpgrade(),
                    issue.getMeta());
            }).collect(Collectors.toList());
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        static final Set<String> RESERVED_NAMES = Sets.newHashSet("cluster_settings", "node_settings", "index_settings");
        private final List<DeprecationIssue> clusterSettingsIssues;
        private final List<DeprecationIssue> nodeSettingsIssues;
        private final Map<String, List<DeprecationIssue>> indexSettingsIssues;
        private final Map<String, List<DeprecationIssue>> pluginSettingsIssues;

        public Response(StreamInput in) throws IOException {
            super(in);
            clusterSettingsIssues = in.readList(DeprecationIssue::new);
            nodeSettingsIssues = in.readList(DeprecationIssue::new);
            indexSettingsIssues = in.readMapOfLists(StreamInput::readString, DeprecationIssue::new);
            if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
                if (in.getVersion().before(Version.V_7_11_0)) {
                    List<DeprecationIssue> mlIssues = in.readList(DeprecationIssue::new);
                    pluginSettingsIssues = new HashMap<>();
                    pluginSettingsIssues.put("ml_settings", mlIssues);
                } else {
                    pluginSettingsIssues = in.readMapOfLists(StreamInput::readString, DeprecationIssue::new);
                }
            } else {
                pluginSettingsIssues = Collections.singletonMap("ml_settings", Collections.emptyList());
            }
        }

        public Response(List<DeprecationIssue> clusterSettingsIssues,
                        List<DeprecationIssue> nodeSettingsIssues,
                        Map<String, List<DeprecationIssue>> indexSettingsIssues,
                        Map<String, List<DeprecationIssue>> pluginSettingsIssues) {
            this.clusterSettingsIssues = clusterSettingsIssues;
            this.nodeSettingsIssues = nodeSettingsIssues;
            this.indexSettingsIssues = indexSettingsIssues;
            Set<String> intersection = Sets.intersection(RESERVED_NAMES, pluginSettingsIssues.keySet());
            if (intersection.isEmpty() == false) {
                throw new ElasticsearchStatusException(
                    "Unable to discover deprecations as plugin deprecation names overlap with reserved names {}",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    intersection
                );
            }
            this.pluginSettingsIssues = pluginSettingsIssues;
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

        public Map<String, List<DeprecationIssue>> getPluginSettingsIssues() {
            return pluginSettingsIssues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(clusterSettingsIssues);
            out.writeList(nodeSettingsIssues);
            out.writeMapOfLists(indexSettingsIssues, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
                if (out.getVersion().before(Version.V_7_11_0)) {
                    out.writeList(pluginSettingsIssues.getOrDefault("ml_settings", Collections.emptyList()));
                } else {
                    out.writeMapOfLists(pluginSettingsIssues, StreamOutput::writeString, (o, v) -> v.writeTo(o));
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .array("cluster_settings", clusterSettingsIssues.toArray())
                .array("node_settings", nodeSettingsIssues.toArray())
                .field("index_settings")
                .map(indexSettingsIssues)
                .mapContents(pluginSettingsIssues)
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
                Objects.equals(pluginSettingsIssues, response.pluginSettingsIssues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues, pluginSettingsIssues);
        }

        /**
         * This is the function that does the bulk of the logic of taking the appropriate ES dependencies
         * like {@link NodeInfo}, {@link ClusterState}. Alongside these objects and the list of deprecation checks,
         * this function will run through all the checks and build out the final list of issues that exist in the
         * cluster.
         *
         * @param state The cluster state
         * @param indexNameExpressionResolver Used to resolve indices into their concrete names
         * @param request The originating request containing the index expressions to evaluate
         * @param nodeDeprecationResponse The response containing the deprecation issues found on each node
         * @param indexSettingsChecks The list of index-level checks that will be run across all specified
         *                            concrete indices
         * @param clusterSettingsChecks The list of cluster-level checks
         * @return The list of deprecation issues found in the cluster
         */
        public static DeprecationInfoAction.Response from(ClusterState state,
                                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                                          Request request,
                                                          NodesDeprecationCheckResponse nodeDeprecationResponse,
                                                          List<Function<IndexMetadata, DeprecationIssue>> indexSettingsChecks,
                                                          List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks,
                                                          Map<String, List<DeprecationIssue>> pluginSettingIssues) {
            List<DeprecationIssue> clusterSettingsIssues = filterChecks(clusterSettingsChecks,
                (c) -> c.apply(state));
            List<DeprecationIssue> nodeSettingsIssues = mergeNodeIssues(nodeDeprecationResponse);

            String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, request);

            Map<String, List<DeprecationIssue>> indexSettingsIssues = new HashMap<>();
            for (String concreteIndex : concreteIndexNames) {
                IndexMetadata indexMetadata = state.getMetadata().index(concreteIndex);
                List<DeprecationIssue> singleIndexIssues = filterChecks(indexSettingsChecks,
                    c -> c.apply(indexMetadata));
                if (singleIndexIssues.size() > 0) {
                    indexSettingsIssues.put(concreteIndex, singleIndexIssues);
                }
            }

            return new DeprecationInfoAction.Response(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues, pluginSettingIssues);
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, true, true, true);
        private String[] indices;

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
        public boolean includeDataStreams() {
            return true;
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

}
