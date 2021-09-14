/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestStats;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TrainedModelStats implements ToXContentObject {

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField PIPELINE_COUNT = new ParseField("pipeline_count");
    public static final ParseField INGEST_STATS = new ParseField("ingest");
    public static final ParseField INFERENCE_STATS = new ParseField("inference_stats");

    private final String modelId;
    private final Map<String, Object> ingestStats;
    private final int pipelineCount;
    private final InferenceStats inferenceStats;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<TrainedModelStats, Void> PARSER =
        new ConstructingObjectParser<>(
            "trained_model_stats",
            true,
            args -> new TrainedModelStats((String) args[0], (Map<String, Object>) args[1], (Integer) args[2], (InferenceStats) args[3]));

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), INGEST_STATS);
        PARSER.declareInt(constructorArg(), PIPELINE_COUNT);
        PARSER.declareObject(optionalConstructorArg(), InferenceStats.PARSER, INFERENCE_STATS);
    }

    public static TrainedModelStats fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public TrainedModelStats(String modelId, Map<String, Object> ingestStats, int pipelineCount, InferenceStats inferenceStats) {
        this.modelId = modelId;
        this.ingestStats = ingestStats;
        this.pipelineCount = pipelineCount;
        this.inferenceStats = inferenceStats;
    }

    /**
     * The model id for which the stats apply
     */
    public String getModelId() {
        return modelId;
    }

    /**
     * Ingest level statistics. See {@link IngestStats#toXContent(XContentBuilder, Params)} for fields and format
     * If there are no ingest pipelines referencing the model, then the ingest statistics could be null.
     */
    @Nullable
    public Map<String, Object> getIngestStats() {
        return ingestStats;
    }

    /**
     * The total number of pipelines that reference the trained model
     */
    public int getPipelineCount() {
        return pipelineCount;
    }

    /**
     * Inference statistics
     */
    public InferenceStats getInferenceStats() {
        return inferenceStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(PIPELINE_COUNT.getPreferredName(), pipelineCount);
        if (ingestStats != null) {
            builder.field(INGEST_STATS.getPreferredName(), ingestStats);
        }
        if (inferenceStats != null) {
            builder.field(INFERENCE_STATS.getPreferredName(), inferenceStats);
        }
        builder.endObject();
        return builder;
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
