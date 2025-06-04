/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeprecationInfoAction extends ActionType<DeprecationInfoAction.Response> {

    public static final DeprecationInfoAction INSTANCE = new DeprecationInfoAction();
    public static final String NAME = "cluster:admin/xpack/deprecation/info";

    private DeprecationInfoAction() {
        super(NAME);
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
            clusterSettingsIssues = in.readCollectionAsList(DeprecationIssue::new);
            nodeSettingsIssues = in.readCollectionAsList(DeprecationIssue::new);
            Map<String, Map<String, List<DeprecationIssue>>> mutableResourceDeprecations = in.getTransportVersion()
                .before(TransportVersions.RESOURCE_DEPRECATION_CHECKS) ? new HashMap<>() : Map.of();
            if (in.getTransportVersion().before(TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                mutableResourceDeprecations.put(IndexDeprecationChecker.NAME, in.readMapOfLists(DeprecationIssue::new));
            }
            if (in.getTransportVersion().between(TransportVersions.V_8_17_0, TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                mutableResourceDeprecations.put(DataStreamDeprecationChecker.NAME, in.readMapOfLists(DeprecationIssue::new));
            }
            pluginSettingsIssues = in.readMapOfLists(DeprecationIssue::new);
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
            if (out.getTransportVersion().between(TransportVersions.V_8_17_0, TransportVersions.RESOURCE_DEPRECATION_CHECKS)) {
                out.writeMap(getDataStreamDeprecationIssues(), StreamOutput::writeCollection);
            }
            out.writeMap(pluginSettingsIssues, StreamOutput::writeCollection);
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
