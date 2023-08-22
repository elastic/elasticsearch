/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    /**
     * This method rolls up DeprecationIssues that are identical but on different nodes. It also roles up DeprecationIssues that are
     * identical (and on different nodes) except that they differ in the removable settings listed in their meta object. We roll these up
     * by taking the intersection of all removable settings in otherwise identical DeprecationIssues. That way we don't claim that a
     * setting can be automatically removed if any node has it in its elasticsearch.yml.
     * @param response
     * @return
     */
    private static List<DeprecationIssue> mergeNodeIssues(NodesDeprecationCheckResponse response) {
        // A collection whose values are lists of DeprecationIssues that differ only by meta values (if that):
        Collection<List<Tuple<DeprecationIssue, String>>> issuesToMerge = getDeprecationIssuesThatDifferOnlyByMeta(response.getNodes());
        // A map of DeprecationIssues (containing only the intersection of removable settings) to the nodes they are seen on
        Map<DeprecationIssue, List<String>> issueToListOfNodesMap = getMergedIssuesToNodesMap(issuesToMerge);

        return issueToListOfNodesMap.entrySet().stream().map(entry -> {
            DeprecationIssue issue = entry.getKey();
            String details = issue.getDetails() != null ? issue.getDetails() + " " : "";
            return new DeprecationIssue(
                issue.getLevel(),
                issue.getMessage(),
                issue.getUrl(),
                details + "(nodes impacted: " + entry.getValue() + ")",
                issue.isResolveDuringRollingUpgrade(),
                issue.getMeta()
            );
        }).collect(Collectors.toList());
    }

    /*
     * This method pulls all of the DeprecationIssues from the given nodeResponses, and buckets them into lists of DeprecationIssues that
     * differ at most by meta values (if that). The returned tuples also contain the node name the deprecation issue was found on. If all
     * nodes in the cluster were configured identically then all tuples in a list will differ only by the node name.
     */
    private static Collection<List<Tuple<DeprecationIssue, String>>> getDeprecationIssuesThatDifferOnlyByMeta(
        List<NodesDeprecationCheckAction.NodeResponse> nodeResponses
    ) {
        Map<DeprecationIssue, List<Tuple<DeprecationIssue, String>>> issuesToMerge = new HashMap<>();
        for (NodesDeprecationCheckAction.NodeResponse resp : nodeResponses) {
            for (DeprecationIssue issue : resp.getDeprecationIssues()) {
                issuesToMerge.computeIfAbsent(
                    new DeprecationIssue(
                        issue.getLevel(),
                        issue.getMessage(),
                        issue.getUrl(),
                        issue.getDetails(),
                        issue.isResolveDuringRollingUpgrade(),
                        null // Intentionally removing meta from the key so that it's not taken into account for equality
                    ),
                    (key) -> new ArrayList<>()
                ).add(new Tuple<>(issue, resp.getNode().getName()));
            }
        }
        return issuesToMerge.values();
    }

    /*
     * At this point we have one DeprecationIssue per node for a given deprecation. This method rolls them up into a single DeprecationIssue
     * with a list of nodes that they appear on. If two DeprecationIssues on two different nodes differ only by the set of removable
     * settings (i.e. they have different elasticsearch.yml configurations) then this method takes the intersection of those settings when
     * it rolls them up.
     */
    private static Map<DeprecationIssue, List<String>> getMergedIssuesToNodesMap(
        Collection<List<Tuple<DeprecationIssue, String>>> issuesToMerge
    ) {
        Map<DeprecationIssue, List<String>> issueToListOfNodesMap = new HashMap<>();
        for (List<Tuple<DeprecationIssue, String>> similarIssues : issuesToMerge) {
            DeprecationIssue leastCommonDenominator = DeprecationIssue.getIntersectionOfRemovableSettings(
                similarIssues.stream().map(Tuple::v1).toList()
            );
            issueToListOfNodesMap.computeIfAbsent(leastCommonDenominator, (key) -> new ArrayList<>())
                .addAll(similarIssues.stream().map(Tuple::v2).toList());
        }
        return issueToListOfNodesMap;
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        static final Set<String> RESERVED_NAMES = Set.of("cluster_settings", "node_settings", "index_settings");
        private final List<DeprecationIssue> clusterSettingsIssues;
        private final List<DeprecationIssue> nodeSettingsIssues;
        private final Map<String, List<DeprecationIssue>> indexSettingsIssues;
        private final Map<String, List<DeprecationIssue>> pluginSettingsIssues;

        public Response(StreamInput in) throws IOException {
            super(in);
            clusterSettingsIssues = in.readList(DeprecationIssue::new);
            nodeSettingsIssues = in.readList(DeprecationIssue::new);
            indexSettingsIssues = in.readMapOfLists(DeprecationIssue::new);
            if (in.getTransportVersion().before(TransportVersion.V_7_11_0)) {
                List<DeprecationIssue> mlIssues = in.readList(DeprecationIssue::new);
                pluginSettingsIssues = new HashMap<>();
                pluginSettingsIssues.put("ml_settings", mlIssues);
            } else {
                pluginSettingsIssues = in.readMapOfLists(DeprecationIssue::new);
            }
        }

        public Response(
            List<DeprecationIssue> clusterSettingsIssues,
            List<DeprecationIssue> nodeSettingsIssues,
            Map<String, List<DeprecationIssue>> indexSettingsIssues,
            Map<String, List<DeprecationIssue>> pluginSettingsIssues
        ) {
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
            if (out.getTransportVersion().before(TransportVersion.V_7_11_0)) {
                out.writeList(pluginSettingsIssues.getOrDefault("ml_settings", Collections.emptyList()));
            } else {
                out.writeMapOfLists(pluginSettingsIssues, StreamOutput::writeString, (o, v) -> v.writeTo(o));
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
            return Objects.equals(clusterSettingsIssues, response.clusterSettingsIssues)
                && Objects.equals(nodeSettingsIssues, response.nodeSettingsIssues)
                && Objects.equals(indexSettingsIssues, response.indexSettingsIssues)
                && Objects.equals(pluginSettingsIssues, response.pluginSettingsIssues);
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
        public static DeprecationInfoAction.Response from(
            ClusterState state,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Request request,
            NodesDeprecationCheckResponse nodeDeprecationResponse,
            List<Function<IndexMetadata, DeprecationIssue>> indexSettingsChecks,
            List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks,
            Map<String, List<DeprecationIssue>> pluginSettingIssues,
            List<String> skipTheseDeprecatedSettings
        ) {
            assert Transports.assertNotTransportThread("walking mappings in indexSettingsChecks is expensive");
            // Allow system index access here to prevent deprecation warnings when we call this API
            String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, request);
            ClusterState stateWithSkippedSettingsRemoved = removeSkippedSettings(state, concreteIndexNames, skipTheseDeprecatedSettings);
            List<DeprecationIssue> clusterSettingsIssues = filterChecks(
                clusterSettingsChecks,
                (c) -> c.apply(stateWithSkippedSettingsRemoved)
            );
            List<DeprecationIssue> nodeSettingsIssues = mergeNodeIssues(nodeDeprecationResponse);

            Map<String, List<DeprecationIssue>> indexSettingsIssues = new HashMap<>();
            for (String concreteIndex : concreteIndexNames) {
                IndexMetadata indexMetadata = stateWithSkippedSettingsRemoved.getMetadata().index(concreteIndex);
                List<DeprecationIssue> singleIndexIssues = filterChecks(indexSettingsChecks, c -> c.apply(indexMetadata));
                if (singleIndexIssues.size() > 0) {
                    indexSettingsIssues.put(concreteIndex, singleIndexIssues);
                }
            }

            // WORKAROUND: move transform deprecation issues into cluster_settings
            List<DeprecationIssue> transformDeprecations = pluginSettingIssues.remove(
                TransformDeprecationChecker.TRANSFORM_DEPRECATION_KEY
            );
            if (transformDeprecations != null) {
                clusterSettingsIssues.addAll(transformDeprecations);
            }

            return new DeprecationInfoAction.Response(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues, pluginSettingIssues);
        }
    }

    /**
     *
     * @param state The cluster state to modify
     * @param indexNames The names of the indexes whose settings need to be filtered
     * @param skipTheseDeprecatedSettings The settings that will be removed from cluster metadata and the index metadata of all the
     *                                    indexes specified by indexNames
     * @return A modified cluster state with the given settings removed
     */
    private static ClusterState removeSkippedSettings(ClusterState state, String[] indexNames, List<String> skipTheseDeprecatedSettings) {
        ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
        metadataBuilder.transientSettings(
            metadataBuilder.transientSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
        );
        metadataBuilder.persistentSettings(
            metadataBuilder.persistentSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
        );
        Map<String, IndexMetadata> indicesBuilder = new HashMap<>(state.getMetadata().indices());
        for (String indexName : indexNames) {
            IndexMetadata indexMetadata = state.getMetadata().index(indexName);
            IndexMetadata.Builder filteredIndexMetadataBuilder = new IndexMetadata.Builder(indexMetadata);
            Settings filteredSettings = indexMetadata.getSettings()
                .filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false);
            filteredIndexMetadataBuilder.settings(filteredSettings);
            indicesBuilder.put(indexName, filteredIndexMetadataBuilder.build());
        }
        metadataBuilder.indices(indicesBuilder);
        clusterStateBuilder.metadata(metadataBuilder);
        return clusterStateBuilder.build();
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
