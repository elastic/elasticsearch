/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.client.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * For building inference pipeline aggregations
 *
 * NOTE: This extends {@linkplain AbstractPipelineAggregationBuilder} for compatibility
 * with {@link SearchSourceBuilder#aggregation(PipelineAggregationBuilder)} but it
 * doesn't support any "server" side things like {@linkplain #doWriteTo(StreamOutput)}
 * or {@linkplain #createInternal(Map)}
 */
public class InferencePipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<InferencePipelineAggregationBuilder> {

    public static String NAME = "inference";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");


    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferencePipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME, false,
        (args, name) -> new InferencePipelineAggregationBuilder(name, (String)args[0], (Map<String, String>) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareObject(constructorArg(), (p, c) -> p.mapStrings(), BUCKETS_PATH_FIELD);
        PARSER.declareNamedObject(InferencePipelineAggregationBuilder::setInferenceConfig,
            (p, c, n) -> p.namedObject(InferenceConfig.class, n, c), INFERENCE_CONFIG);
    }

    private final Map<String, String> bucketPathMap;
    private final String modelId;
    private InferenceConfig inferenceConfig;

    public static InferencePipelineAggregationBuilder parse(String pipelineAggregatorName,
                                                            XContentParser parser) {
        return PARSER.apply(parser, pipelineAggregatorName);
    }

    public InferencePipelineAggregationBuilder(String name, String modelId, Map<String, String> bucketsPath) {
        super(name, NAME, new TreeMap<>(bucketsPath).values().toArray(new String[] {}));
        this.modelId = modelId;
        this.bucketPathMap = bucketsPath;
    }

    public void setInferenceConfig(InferenceConfig inferenceConfig) {
        this.inferenceConfig = inferenceConfig;
    }

    @Override
    protected void validate(ValidationContext context) {
        // validation occurs on the server
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketPathMap);
        if (inferenceConfig != null) {
            builder.startObject(INFERENCE_CONFIG.getPreferredName());
            builder.field(inferenceConfig.getName(), inferenceConfig);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketPathMap, modelId, inferenceConfig);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InferencePipelineAggregationBuilder other = (InferencePipelineAggregationBuilder) obj;
        return Objects.equals(bucketPathMap, other.bucketPathMap)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(inferenceConfig, other.inferenceConfig);
    }
}
