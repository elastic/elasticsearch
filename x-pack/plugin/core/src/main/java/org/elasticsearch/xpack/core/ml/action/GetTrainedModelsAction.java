/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


public class GetTrainedModelsAction extends ActionType<GetTrainedModelsAction.Response> {

    public static final GetTrainedModelsAction INSTANCE = new GetTrainedModelsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/inference/get";

    private GetTrainedModelsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        static final String DEFINITION = "definition";
        static final String TOTAL_FEATURE_IMPORTANCE = "total_feature_importance";
        private static final Set<String> KNOWN_INCLUDES;
        static {
            HashSet<String> includes = new HashSet<>(2, 1.0f);
            includes.add(DEFINITION);
            includes.add(TOTAL_FEATURE_IMPORTANCE);
            KNOWN_INCLUDES = Collections.unmodifiableSet(includes);
        }
        public static final ParseField INCLUDE = new ParseField("include");
        public static final String INCLUDE_MODEL_DEFINITION = "include_model_definition";
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
        public static final ParseField TAGS = new ParseField("tags");

        private final Set<String> includes;
        private final List<String> tags;

        @Deprecated
        public Request(String id, boolean includeModelDefinition, List<String> tags) {
            setResourceId(id);
            setAllowNoResources(true);
            this.tags = tags == null ? Collections.emptyList() : tags;
            if (includeModelDefinition) {
                this.includes = new HashSet<>(Collections.singletonList(DEFINITION));
            } else {
                this.includes = Collections.emptySet();
            }
        }

        public Request(String id, List<String> tags, Set<String> includes) {
            setResourceId(id);
            setAllowNoResources(true);
            this.tags = tags == null ? Collections.emptyList() : tags;
            this.includes = includes == null ? Collections.emptySet() : includes;
            Set<String> unknownIncludes = Sets.difference(this.includes, KNOWN_INCLUDES);
            if (unknownIncludes.isEmpty() == false) {
                throw ExceptionsHelper.badRequestException(
                    "unknown [include] parameters {}. Valid options are {}",
                    unknownIncludes,
                    KNOWN_INCLUDES);
            }
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
                this.includes = in.readSet(StreamInput::readString);
            } else {
                Set<String> includes = new HashSet<>();
                if (in.readBoolean()) {
                    includes.add(DEFINITION);
                }
                this.includes = includes;
            }
            this.tags = in.readStringList();
        }

        @Override
        public String getResourceIdField() {
            return TrainedModelConfig.MODEL_ID.getPreferredName();
        }

        public boolean isIncludeModelDefinition() {
            return this.includes.contains(DEFINITION);
        }

        public boolean isIncludeTotalFeatureImportance() {
            return this.includes.contains(TOTAL_FEATURE_IMPORTANCE);
        }

        public List<String> getTags() {
            return tags;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
                out.writeCollection(this.includes, StreamOutput::writeString);
            } else {
                out.writeBoolean(this.includes.contains(DEFINITION));
            }
            out.writeStringCollection(tags);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), includes, tags);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return super.equals(obj) && this.includes.equals(other.includes) && Objects.equals(tags, other.tags);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<TrainedModelConfig> {

        public static final ParseField RESULTS_FIELD = new ParseField("trained_model_configs");

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<TrainedModelConfig> trainedModels) {
            super(trainedModels);
        }

        @Override
        protected Reader<TrainedModelConfig> getReader() {
            return TrainedModelConfig::new;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private long totalCount;
            private List<TrainedModelConfig> configs = Collections.emptyList();

            private Builder() {
            }

            public Builder setTotalCount(long totalCount) {
                this.totalCount = totalCount;
                return this;
            }

            public Builder setModels(List<TrainedModelConfig> configs) {
                this.configs = configs;
                return this;
            }

            public Response build() {
                return new Response(new QueryPage<>(configs, totalCount, RESULTS_FIELD));
            }
        }
    }

}
