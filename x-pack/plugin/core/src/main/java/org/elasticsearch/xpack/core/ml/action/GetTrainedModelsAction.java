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
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class GetTrainedModelsAction extends ActionType<GetTrainedModelsAction.Response> {

    public static final GetTrainedModelsAction INSTANCE = new GetTrainedModelsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/inference/get";

    private GetTrainedModelsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        public static final ParseField INCLUDE_MODEL_DEFINITION = new ParseField("include_model_definition");
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
        public static final ParseField TAGS = new ParseField("tags");

        private final boolean includeModelDefinition;
        private final List<String> tags;

        public Request(String id, boolean includeModelDefinition, List<String> tags) {
            setResourceId(id);
            setAllowNoResources(true);
            this.includeModelDefinition = includeModelDefinition;
            this.tags = tags == null ? Collections.emptyList() : tags;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.includeModelDefinition = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                this.tags = in.readStringList();
            } else {
                this.tags = Collections.emptyList();
            }
        }

        @Override
        public String getResourceIdField() {
            return TrainedModelConfig.MODEL_ID.getPreferredName();
        }

        public boolean isIncludeModelDefinition() {
            return includeModelDefinition;
        }

        public List<String> getTags() {
            return tags;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(includeModelDefinition);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeStringCollection(tags);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), includeModelDefinition, tags);
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
            return super.equals(obj) && this.includeModelDefinition == other.includeModelDefinition && Objects.equals(tags, other.tags);
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
