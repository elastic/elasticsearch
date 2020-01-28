/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
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

    private final String modelId;
    private final Map<String, Object> ingestStats;
    private final int pipelineCount;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<TrainedModelStats, Void> PARSER =
        new ConstructingObjectParser<>(
            "trained_model_stats",
            true,
            args -> new TrainedModelStats((String) args[0], (Map<String, Object>) args[1], (Integer) args[2]));

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), INGEST_STATS);
        PARSER.declareInt(constructorArg(), PIPELINE_COUNT);
    }

    public static TrainedModelStats fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public TrainedModelStats(String modelId, Map<String, Object> ingestStats, int pipelineCount) {
        this.modelId = modelId;
        this.ingestStats = ingestStats;
        this.pipelineCount = pipelineCount;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(PIPELINE_COUNT.getPreferredName(), pipelineCount);
        if (ingestStats != null) {
            builder.field(INGEST_STATS.getPreferredName(), ingestStats);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, ingestStats, pipelineCount);
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
            && Objects.equals(this.pipelineCount, other.pipelineCount);
    }

}
