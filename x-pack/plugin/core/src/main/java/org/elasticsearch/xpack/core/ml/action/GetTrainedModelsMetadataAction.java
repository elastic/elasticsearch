/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GetTrainedModelsMetadataAction extends ActionType<GetTrainedModelsMetadataAction.Response> {

    public static final GetTrainedModelsMetadataAction INSTANCE = new GetTrainedModelsMetadataAction();
    public static final String NAME = "cluster:monitor/xpack/ml/inference/metadata/get";

    private GetTrainedModelsMetadataAction() {
        super(NAME, GetTrainedModelsMetadataAction.Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

        public Request() {
            setAllowNoResources(true);
        }

        public Request(String id) {
            setResourceId(id);
            setAllowNoResources(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getResourceIdField() {
            return TrainedModelConfig.MODEL_ID.getPreferredName();
        }

    }

    public static class Response extends AbstractGetResourcesResponse<TrainedModelMetadata> {

        public static final ParseField RESULTS_FIELD = new ParseField("trained_models_metadata");

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<TrainedModelMetadata> trainedModels) {
            super(trainedModels);
        }

        @Override
        protected Reader<TrainedModelMetadata> getReader() {
            return TrainedModelMetadata::new;
        }

        public static class Builder {

            private long totalModelCount;
            private Set<String> expandedIds;
            private Map<String, TrainedModelMetadata> trainedModelMetadataMap;

            public Builder setTotalModelCount(long totalModelCount) {
                this.totalModelCount = totalModelCount;
                return this;
            }

            public Builder setExpandedIds(Set<String> expandedIds) {
                this.expandedIds = expandedIds;
                return this;
            }

            public Set<String> getExpandedIds() {
                return this.expandedIds;
            }

            public Builder setTrainedModelMetadata(Map<String, TrainedModelMetadata> modelMetadataByModelId) {
                this.trainedModelMetadataMap = modelMetadataByModelId;
                return this;
            }

            public Response build() {
                return new Response(new QueryPage<>(
                    expandedIds.stream()
                        .map(trainedModelMetadataMap::get)
                        .filter(Objects::nonNull)
                        .sorted(Comparator.comparing(TrainedModelMetadata::getModelId))
                        .collect(Collectors.toList()),
                    totalModelCount,
                    RESULTS_FIELD));
            }
        }
    }

}
