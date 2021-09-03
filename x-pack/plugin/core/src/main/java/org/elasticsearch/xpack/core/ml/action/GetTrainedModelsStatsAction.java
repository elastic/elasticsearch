/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GetTrainedModelsStatsAction extends ActionType<GetTrainedModelsStatsAction.Response> {

    public static final GetTrainedModelsStatsAction INSTANCE = new GetTrainedModelsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/inference/stats/get";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField PIPELINE_COUNT = new ParseField("pipeline_count");
    public static final ParseField INFERENCE_STATS = new ParseField("inference_stats");

    private GetTrainedModelsStatsAction() {
        super(NAME, GetTrainedModelsStatsAction.Response::new);
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

    public static class Response extends AbstractGetResourcesResponse<Response.TrainedModelStats> {

        public static class TrainedModelStats implements ToXContentObject, Writeable {
            private final String modelId;
            private final IngestStats ingestStats;
            private final InferenceStats inferenceStats;
            private final int pipelineCount;

            private static final IngestStats EMPTY_INGEST_STATS = new IngestStats(new IngestStats.Stats(0, 0, 0, 0),
                Collections.emptyList(),
                Collections.emptyMap());

            public TrainedModelStats(String modelId, IngestStats ingestStats, int pipelineCount, InferenceStats inferenceStats) {
                this.modelId = Objects.requireNonNull(modelId);
                this.ingestStats = ingestStats == null ? EMPTY_INGEST_STATS : ingestStats;
                if (pipelineCount < 0) {
                    throw new ElasticsearchException("[{}] must be a greater than or equal to 0", PIPELINE_COUNT.getPreferredName());
                }
                this.pipelineCount = pipelineCount;
                this.inferenceStats = inferenceStats;
            }

            public TrainedModelStats(StreamInput in) throws IOException {
                modelId = in.readString();
                ingestStats = new IngestStats(in);
                pipelineCount = in.readVInt();
                this.inferenceStats = in.readOptionalWriteable(InferenceStats::new);
            }

            public String getModelId() {
                return modelId;
            }

            public IngestStats getIngestStats() {
                return ingestStats;
            }

            public int getPipelineCount() {
                return pipelineCount;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(MODEL_ID.getPreferredName(), modelId);
                builder.field(PIPELINE_COUNT.getPreferredName(), pipelineCount);
                if (pipelineCount > 0) {
                    // Ingest stats is a fragment
                    ingestStats.toXContent(builder, params);
                }
                if (this.inferenceStats != null) {
                    builder.field(INFERENCE_STATS.getPreferredName(), this.inferenceStats);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(modelId);
                ingestStats.writeTo(out);
                out.writeVInt(pipelineCount);
                out.writeOptionalWriteable(this.inferenceStats);
            }

            @Override
            public int hashCode() {
                return Objects.hash(modelId, ingestStats, pipelineCount, inferenceStats);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                TrainedModelStats other = (TrainedModelStats) obj;
                return Objects.equals(this.modelId, other.modelId)
                    && Objects.equals(this.ingestStats, other.ingestStats)
                    && Objects.equals(this.pipelineCount, other.pipelineCount)
                    && Objects.equals(this.inferenceStats, other.inferenceStats);
            }
        }

        public static final ParseField RESULTS_FIELD = new ParseField("trained_model_stats");

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<Response.TrainedModelStats> trainedModels) {
            super(trainedModels);
        }

        @Override
        protected Reader<Response.TrainedModelStats> getReader() {
            return Response.TrainedModelStats::new;
        }

        public static class Builder {

            private long totalModelCount;
            private Map<String, Set<String>> expandedIdsWithAliases;
            private Map<String, IngestStats> ingestStatsMap;
            private Map<String, InferenceStats> inferenceStatsMap;

            public Builder setTotalModelCount(long totalModelCount) {
                this.totalModelCount = totalModelCount;
                return this;
            }

            public Builder setExpandedIdsWithAliases(Map<String, Set<String>> expandedIdsWithAliases) {
                this.expandedIdsWithAliases = expandedIdsWithAliases;
                return this;
            }

            public Map<String, Set<String>> getExpandedIdsWithAliases() {
                return this.expandedIdsWithAliases;
            }

            public Builder setIngestStatsByModelId(Map<String, IngestStats> ingestStatsByModelId) {
                this.ingestStatsMap = ingestStatsByModelId;
                return this;
            }

            public Builder setInferenceStatsByModelId(Map<String, InferenceStats> infereceStatsByModelId) {
                this.inferenceStatsMap = infereceStatsByModelId;
                return this;
            }

            public Response build() {
                List<TrainedModelStats> trainedModelStats = new ArrayList<>(expandedIdsWithAliases.size());
                expandedIdsWithAliases.keySet().forEach(id -> {
                    IngestStats ingestStats = ingestStatsMap.get(id);
                    InferenceStats inferenceStats = inferenceStatsMap.get(id);
                    trainedModelStats.add(new TrainedModelStats(
                        id,
                        ingestStats,
                        ingestStats == null ?
                            0 :
                            ingestStats.getPipelineStats().size(),
                        inferenceStats));
                });
                trainedModelStats.sort(Comparator.comparing(TrainedModelStats::getModelId));
                return new Response(new QueryPage<>(trainedModelStats, totalModelCount, RESULTS_FIELD));
            }
        }
    }

}
