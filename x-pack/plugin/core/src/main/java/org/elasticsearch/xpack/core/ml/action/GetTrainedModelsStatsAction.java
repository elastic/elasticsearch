/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.core.RestApiVersion.onOrAfter;
import static org.elasticsearch.core.Strings.format;

public class GetTrainedModelsStatsAction extends ActionType<GetTrainedModelsStatsAction.Response> {

    public static final GetTrainedModelsStatsAction INSTANCE = new GetTrainedModelsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/inference/stats/get";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField MODEL_SIZE_STATS = new ParseField("model_size_stats");
    public static final ParseField PIPELINE_COUNT = new ParseField("pipeline_count");
    public static final ParseField INFERENCE_STATS = new ParseField("inference_stats");
    public static final ParseField DEPLOYMENT_STATS = new ParseField("deployment_stats");

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
        public String getCancelableTaskDescription() {
            return format("get_trained_model_stats[%s]", getResourceId());
        }

        @Override
        public String getResourceIdField() {
            return TrainedModelConfig.MODEL_ID.getPreferredName();
        }

    }

    public static class Response extends AbstractGetResourcesResponse<Response.TrainedModelStats> {

        public static class TrainedModelStats implements ToXContentObject, Writeable {
            private final String modelId;
            private final TrainedModelSizeStats modelSizeStats;
            private final IngestStats ingestStats;
            private final InferenceStats inferenceStats;
            private final AssignmentStats deploymentStats;
            private final int pipelineCount;

            private static final IngestStats EMPTY_INGEST_STATS = new IngestStats(
                new IngestStats.Stats(0, 0, 0, 0),
                Collections.emptyList(),
                Collections.emptyMap()
            );

            public TrainedModelStats(
                String modelId,
                TrainedModelSizeStats modelSizeStats,
                IngestStats ingestStats,
                int pipelineCount,
                InferenceStats inferenceStats,
                AssignmentStats deploymentStats
            ) {
                this.modelId = Objects.requireNonNull(modelId);
                this.modelSizeStats = modelSizeStats;
                this.ingestStats = ingestStats == null ? EMPTY_INGEST_STATS : ingestStats;
                if (pipelineCount < 0) {
                    throw new ElasticsearchException("[{}] must be a greater than or equal to 0", PIPELINE_COUNT.getPreferredName());
                }
                this.pipelineCount = pipelineCount;
                this.inferenceStats = inferenceStats;
                this.deploymentStats = deploymentStats;
            }

            public TrainedModelStats(StreamInput in) throws IOException {
                modelId = in.readString();
                if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
                    modelSizeStats = in.readOptionalWriteable(TrainedModelSizeStats::new);
                } else {
                    modelSizeStats = null;
                }
                ingestStats = new IngestStats(in);
                pipelineCount = in.readVInt();
                inferenceStats = in.readOptionalWriteable(InferenceStats::new);
                if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
                    this.deploymentStats = in.readOptionalWriteable(AssignmentStats::new);
                } else {
                    this.deploymentStats = null;
                }
            }

            public String getModelId() {
                return modelId;
            }

            public TrainedModelSizeStats getModelSizeStats() {
                return modelSizeStats;
            }

            public IngestStats getIngestStats() {
                return ingestStats;
            }

            public int getPipelineCount() {
                return pipelineCount;
            }

            public InferenceStats getInferenceStats() {
                return inferenceStats;
            }

            public AssignmentStats getDeploymentStats() {
                return deploymentStats;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(MODEL_ID.getPreferredName(), modelId);
                if (modelSizeStats != null) {
                    builder.field(MODEL_SIZE_STATS.getPreferredName(), modelSizeStats);
                }
                builder.field(PIPELINE_COUNT.getPreferredName(), pipelineCount);
                if (pipelineCount > 0) {
                    // Ingest stats is a fragment
                    ChunkedToXContent.wrapAsToXContent(ingestStats).toXContent(builder, params);
                }
                if (this.inferenceStats != null) {
                    builder.field(INFERENCE_STATS.getPreferredName(), this.inferenceStats);
                }
                if (deploymentStats != null && builder.getRestApiVersion().matches(onOrAfter(RestApiVersion.V_8))) {
                    builder.field(DEPLOYMENT_STATS.getPreferredName(), this.deploymentStats);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(modelId);
                if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
                    out.writeOptionalWriteable(modelSizeStats);
                }
                ingestStats.writeTo(out);
                out.writeVInt(pipelineCount);
                out.writeOptionalWriteable(inferenceStats);
                if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
                    out.writeOptionalWriteable(deploymentStats);
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(modelId, modelSizeStats, ingestStats, pipelineCount, inferenceStats, deploymentStats);
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
                    && Objects.equals(this.modelSizeStats, other.modelSizeStats)
                    && Objects.equals(this.ingestStats, other.ingestStats)
                    && Objects.equals(this.pipelineCount, other.pipelineCount)
                    && Objects.equals(this.deploymentStats, other.deploymentStats)
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
            private Map<String, Set<String>> expandedModelIdsWithAliases;
            private Map<String, TrainedModelSizeStats> modelSizeStatsMap;
            private Map<String, IngestStats> ingestStatsMap;
            private Map<String, InferenceStats> inferenceStatsMap;
            private Map<String, AssignmentStats> assignmentStatsMap;

            public Builder setTotalModelCount(long totalModelCount) {
                this.totalModelCount = totalModelCount;
                return this;
            }

            public Builder setExpandedModelIdsWithAliases(Map<String, Set<String>> expandedIdsWithAliases) {
                this.expandedModelIdsWithAliases = expandedIdsWithAliases;
                return this;
            }

            public Map<String, Set<String>> getExpandedModelIdsWithAliases() {
                return this.expandedModelIdsWithAliases;
            }

            public Builder setModelSizeStatsByModelId(Map<String, TrainedModelSizeStats> modelSizeStatsByModelId) {
                this.modelSizeStatsMap = modelSizeStatsByModelId;
                return this;
            }

            public Builder setIngestStatsByModelId(Map<String, IngestStats> ingestStatsByModelId) {
                this.ingestStatsMap = ingestStatsByModelId;
                return this;
            }

            public Builder setInferenceStatsByModelId(Map<String, InferenceStats> inferenceStatsByModelId) {
                this.inferenceStatsMap = inferenceStatsByModelId;
                return this;
            }

            /**
             * This sets the overall stats map and adds the models to the overall inference stats map
             * @param assignmentStatsMap map of model_id to assignment stats
             * @return the builder with inference stats map updated and assignment stats map set
             */
            public Builder setDeploymentStatsByDeploymentId(Map<String, AssignmentStats> assignmentStatsMap) {
                this.assignmentStatsMap = assignmentStatsMap;
                return this;
            }

            public Response build(Map<String, Set<String>> modelToDeploymentIds) {
                int numResponses = expandedModelIdsWithAliases.size();
                // plus an extra response for every deployment after
                // the first per model
                for (var entry : modelToDeploymentIds.entrySet()) {
                    assert expandedModelIdsWithAliases.containsKey(entry.getKey()); // model id
                    assert entry.getValue().size() > 0; // must have a deployment
                    numResponses += entry.getValue().size() - 1;
                }

                if (inferenceStatsMap == null) {
                    inferenceStatsMap = Collections.emptyMap();
                }

                List<TrainedModelStats> trainedModelStats = new ArrayList<>(numResponses);
                expandedModelIdsWithAliases.keySet().forEach(modelId -> {
                    if (modelToDeploymentIds.containsKey(modelId) == false) { // not deployed
                        TrainedModelSizeStats modelSizeStats = modelSizeStatsMap.get(modelId);
                        IngestStats ingestStats = ingestStatsMap.get(modelId);
                        InferenceStats inferenceStats = inferenceStatsMap.get(modelId);
                        trainedModelStats.add(
                            new TrainedModelStats(
                                modelId,
                                modelSizeStats,
                                ingestStats,
                                ingestStats == null ? 0 : ingestStats.pipelineStats().size(),
                                inferenceStats,
                                null // no assignment stats for undeployed models
                            )
                        );
                    } else {
                        for (var deploymentId : modelToDeploymentIds.get(modelId)) {
                            AssignmentStats assignmentStats = assignmentStatsMap.get(deploymentId);
                            if (assignmentStats == null) {
                                continue;
                            }
                            InferenceStats inferenceStats = assignmentStats.getOverallInferenceStats();
                            IngestStats ingestStats = ingestStatsMap.get(deploymentId);
                            if (ingestStats == null) {
                                // look up by model id
                                ingestStats = ingestStatsMap.get(modelId);
                            }
                            TrainedModelSizeStats modelSizeStats = modelSizeStatsMap.get(modelId);
                            trainedModelStats.add(
                                new TrainedModelStats(
                                    modelId,
                                    modelSizeStats,
                                    ingestStats,
                                    ingestStats == null ? 0 : ingestStats.pipelineStats().size(),
                                    inferenceStats,
                                    assignmentStats
                                )
                            );
                        }
                    }
                });

                // Sort first by model id then by deployment id
                trainedModelStats.sort((modelStats1, modelStats2) -> {
                    var comparison = modelStats1.getModelId().compareTo(modelStats2.getModelId());
                    if (comparison == 0) {
                        var deploymentId1 = modelStats1.getDeploymentStats() == null
                            ? null
                            : modelStats1.getDeploymentStats().getDeploymentId();
                        var deploymentId2 = modelStats2.getDeploymentStats() == null
                            ? null
                            : modelStats1.getDeploymentStats().getDeploymentId();

                        assert deploymentId1 != null && deploymentId2 != null
                            : "2 results for model " + modelStats1.getModelId() + " both should have deployment stats";

                        comparison = deploymentId1.compareTo(deploymentId2);
                    }
                    return comparison;
                });
                return new Response(new QueryPage<>(trainedModelStats, totalModelCount, RESULTS_FIELD));
            }
        }
    }

}
