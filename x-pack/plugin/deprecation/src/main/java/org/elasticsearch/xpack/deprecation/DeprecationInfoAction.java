/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
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
     * This method pulls all the DeprecationIssues from the given nodeResponses, and buckets them into lists of DeprecationIssues that
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
        static final Set<String> RESERVED_NAMES = Set.of(
            "cluster_settings",
            "node_settings",
            "index_settings",
            "data_streams",
            "templates",
            "ilm_policies"
        );
        static final Set<String> RESOURCE_CHECKER_FIELD_NAMES = Set.of("index_settings", "data_streams", "templates", "ilm_policies");
        private final List<DeprecationIssue> clusterSettingsIssues;
        private final List<DeprecationIssue> nodeSettingsIssues;
        private final Map<String, Map<String, List<DeprecationIssue>>> resourceDeprecationIssues;
        private final Map<String, List<DeprecationIssue>> pluginSettingsIssues;

        public Response(StreamInput in) throws IOException {
            super(in);
            clusterSettingsIssues = in.readCollectionAsList(DeprecationIssue::new);
            nodeSettingsIssues = in.readCollectionAsList(DeprecationIssue::new);
            Map<String, Map<String, List<DeprecationIssue>>> mutableResourceDeprecations = in.getTransportVersion()
                .before(TransportVersions.RESOURCE_DEPRECATION_CHECKS) ? new HashMap<>() : Map.of();
            if (in.getTransportVersion().before(TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                mutableResourceDeprecations.put(IndexDeprecationChecker.NAME, in.readMapOfLists(DeprecationIssue::new));
            }
            if (in.getTransportVersion()
                .between(TransportVersions.DATA_STREAM_INDEX_VERSION_DEPRECATION_CHECK, TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                mutableResourceDeprecations.put(DataStreamDeprecationChecker.NAME, in.readMapOfLists(DeprecationIssue::new));
            }
            if (in.getTransportVersion().before(TransportVersions.V_7_11_0)) {
                List<DeprecationIssue> mlIssues = in.readCollectionAsList(DeprecationIssue::new);
                pluginSettingsIssues = new HashMap<>();
                pluginSettingsIssues.put("ml_settings", mlIssues);
            } else {
                pluginSettingsIssues = in.readMapOfLists(DeprecationIssue::new);
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                resourceDeprecationIssues = in.readMap(in2 -> in2.readMapOfLists(DeprecationIssue::new));
            } else {
                resourceDeprecationIssues = Collections.unmodifiableMap(mutableResourceDeprecations);
            }
        }

        public Response(
            List<DeprecationIssue> clusterSettingsIssues,
            List<DeprecationIssue> nodeSettingsIssues,
            Map<String, Map<String, List<DeprecationIssue>>> resourceDeprecationIssues,
            Map<String, List<DeprecationIssue>> pluginSettingsIssues
        ) {
            this.clusterSettingsIssues = clusterSettingsIssues;
            this.nodeSettingsIssues = nodeSettingsIssues;
            this.resourceDeprecationIssues = resourceDeprecationIssues;
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
            return resourceDeprecationIssues.getOrDefault(IndexDeprecationChecker.NAME, Map.of());
        }

        public Map<String, List<DeprecationIssue>> getPluginSettingsIssues() {
            return pluginSettingsIssues;
        }

        public Map<String, List<DeprecationIssue>> getDataStreamDeprecationIssues() {
            return resourceDeprecationIssues.getOrDefault(DataStreamDeprecationChecker.NAME, Map.of());
        }

        public Map<String, List<DeprecationIssue>> getTemplateDeprecationIssues() {
            return resourceDeprecationIssues.getOrDefault(TemplateDeprecationChecker.NAME, Map.of());
        }

        public Map<String, List<DeprecationIssue>> getIlmPolicyDeprecationIssues() {
            return resourceDeprecationIssues.getOrDefault(IlmPolicyDeprecationChecker.NAME, Map.of());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(clusterSettingsIssues);
            out.writeCollection(nodeSettingsIssues);
            if (out.getTransportVersion().before(TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                out.writeMap(getIndexSettingsIssues(), StreamOutput::writeCollection);
            }
            if (out.getTransportVersion()
                .between(TransportVersions.DATA_STREAM_INDEX_VERSION_DEPRECATION_CHECK, TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                out.writeMap(getDataStreamDeprecationIssues(), StreamOutput::writeCollection);
            }
            if (out.getTransportVersion().before(TransportVersions.V_7_11_0)) {
                out.writeCollection(pluginSettingsIssues.getOrDefault("ml_settings", Collections.emptyList()));
            } else {
                out.writeMap(pluginSettingsIssues, StreamOutput::writeCollection);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                out.writeMap(resourceDeprecationIssues, (o, v) -> o.writeMap(v, StreamOutput::writeCollection));
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                .array("cluster_settings", clusterSettingsIssues.toArray())
                .array("node_settings", nodeSettingsIssues.toArray())
                .mapContents(resourceDeprecationIssues)
                .mapContents(pluginSettingsIssues);
            // Ensure that all the required fields are present in the response.
            for (String fieldName : RESOURCE_CHECKER_FIELD_NAMES) {
                if (resourceDeprecationIssues.containsKey(fieldName) == false) {
                    builder.startObject(fieldName).endObject();
                }
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(clusterSettingsIssues, response.clusterSettingsIssues)
                && Objects.equals(nodeSettingsIssues, response.nodeSettingsIssues)
                && Objects.equals(resourceDeprecationIssues, response.resourceDeprecationIssues)
                && Objects.equals(pluginSettingsIssues, response.pluginSettingsIssues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterSettingsIssues, nodeSettingsIssues, resourceDeprecationIssues, pluginSettingsIssues);
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
         * @param clusterSettingsChecks The list of cluster-level checks
         * @param pluginSettingIssues this map gets modified to move transform deprecation issues into cluster_settings
         * @param skipTheseDeprecatedSettings the settings that will be removed from cluster metadata and the index metadata of all the
         *                                    indexes specified by indexNames
         * @param resourceDeprecationCheckers these are checkers that take as input the cluster state and return a map from resource type
         *                                    to issues grouped by the resource name.
         * @return The list of deprecation issues found in the cluster
         */
        public static DeprecationInfoAction.Response from(
            ClusterState state,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Request request,
            NodesDeprecationCheckResponse nodeDeprecationResponse,
            List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks,
            Map<String, List<DeprecationIssue>> pluginSettingIssues,
            List<String> skipTheseDeprecatedSettings,
            List<ResourceDeprecationChecker> resourceDeprecationCheckers
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

            Map<String, Map<String, List<DeprecationIssue>>> resourceDeprecationIssues = new HashMap<>();
            for (ResourceDeprecationChecker resourceDeprecationChecker : resourceDeprecationCheckers) {
                Map<String, List<DeprecationIssue>> issues = resourceDeprecationChecker.check(stateWithSkippedSettingsRemoved, request);
                if (issues.isEmpty() == false) {
                    resourceDeprecationIssues.put(resourceDeprecationChecker.getName(), issues);
                }
            }

            // WORKAROUND: move transform deprecation issues into cluster_settings
            List<DeprecationIssue> transformDeprecations = pluginSettingIssues.remove(
                TransformDeprecationChecker.TRANSFORM_DEPRECATION_KEY
            );
            if (transformDeprecations != null) {
                clusterSettingsIssues.addAll(transformDeprecations);
            }

            return new DeprecationInfoAction.Response(
                clusterSettingsIssues,
                nodeSettingsIssues,
                resourceDeprecationIssues,
                pluginSettingIssues
            );
        }
    }

    /**
     * Removes the skipped settings from the selected indices and the component and index templates.
     * @param state The cluster state to modify
     * @param indexNames The names of the indexes whose settings need to be filtered
     * @param skipTheseDeprecatedSettings The settings that will be removed from cluster metadata and the index metadata of all the
     *                                    indexes specified by indexNames
     * @return A modified cluster state with the given settings removed
     */
    private static ClusterState removeSkippedSettings(ClusterState state, String[] indexNames, List<String> skipTheseDeprecatedSettings) {
        // Short-circuit, no need to reconstruct the cluster state if there are no settings to remove
        if (skipTheseDeprecatedSettings == null || skipTheseDeprecatedSettings.isEmpty()) {
            return state;
        }
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
        metadataBuilder.componentTemplates(state.metadata().componentTemplates().entrySet().stream().map(entry -> {
            String templateName = entry.getKey();
            ComponentTemplate componentTemplate = entry.getValue();
            Template template = componentTemplate.template();
            if (template.settings() == null || template.settings().isEmpty()) {
                return Tuple.tuple(templateName, componentTemplate);
            }
            return Tuple.tuple(
                templateName,
                new ComponentTemplate(
                    Template.builder(template)
                        .settings(template.settings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false))
                        .build(),
                    componentTemplate.version(),
                    componentTemplate.metadata(),
                    componentTemplate.deprecated()
                )
            );
        }).collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
        metadataBuilder.indexTemplates(state.metadata().templatesV2().entrySet().stream().map(entry -> {
            String templateName = entry.getKey();
            ComposableIndexTemplate indexTemplate = entry.getValue();
            Template template = indexTemplate.template();
            if (template == null || template.settings() == null || template.settings().isEmpty()) {
                return Tuple.tuple(templateName, indexTemplate);
            }
            return Tuple.tuple(
                templateName,
                indexTemplate.toBuilder()
                    .template(
                        Template.builder(indexTemplate.template())
                            .settings(
                                indexTemplate.template()
                                    .settings()
                                    .filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
                            )
                    )
                    .build()
            );
        }).collect(Collectors.toMap(Tuple::v1, Tuple::v2)));

        metadataBuilder.indices(indicesBuilder);
        clusterStateBuilder.metadata(metadataBuilder);
        return clusterStateBuilder.build();
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, true, true, true, true);
        private String[] indices;

        public Request(TimeValue masterNodeTimeout, String... indices) {
            super(masterNodeTimeout);
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
