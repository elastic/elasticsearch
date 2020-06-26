/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class InferencePipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<InferencePipelineAggregationBuilder> {

    public static String NAME = "inference";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

    static String AGGREGATIONS_RESULTS_FIELD = "value";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferencePipelineAggregationBuilder,
        Tuple<SetOnce<ModelLoadingService>, String>> PARSER = new ConstructingObjectParser<>(
        NAME, false,
        (args, context) -> new InferencePipelineAggregationBuilder(context.v2(), context.v1(), (Map<String, String>) args[0])
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> p.mapStrings(), BUCKETS_PATH_FIELD);
        PARSER.declareString(InferencePipelineAggregationBuilder::setModelId, MODEL_ID);
        PARSER.declareNamedObject(InferencePipelineAggregationBuilder::setInferenceConfig,
            (p, c, n) -> p.namedObject(InferenceConfigUpdate.class, n, c), INFERENCE_CONFIG);
        PARSER.declareField(InferencePipelineAggregationBuilder::setGapPolicy, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return BucketHelpers.GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException(
                "Unsupported token [" + p.currentToken() + "] parsing inference aggregation " + GAP_POLICY.getPreferredName());
        }, GAP_POLICY, ObjectParser.ValueType.STRING);
    }

    private final Map<String, String> bucketPathMap;
    private String modelId;
    private InferenceConfigUpdate inferenceConfig;
    private final SetOnce<ModelLoadingService> modelLoadingService;
    private BucketHelpers.GapPolicy gapPolicy = BucketHelpers.GapPolicy.SKIP;

    public static InferencePipelineAggregationBuilder parse(SetOnce<ModelLoadingService> modelLoadingService,
                                                            String pipelineAggregatorName,
                                                            XContentParser parser) {
        Tuple<SetOnce<ModelLoadingService>, String> context = new Tuple<>(modelLoadingService, pipelineAggregatorName);
        return PARSER.apply(parser, context);
    }

    public InferencePipelineAggregationBuilder(String name, SetOnce<ModelLoadingService> modelLoadingService,
                                               Map<String, String> bucketsPath) {
        super(name, NAME, new TreeMap<>(bucketsPath).values().toArray(new String[] {}));
        this.modelLoadingService = modelLoadingService;
        this.bucketPathMap = bucketsPath;
    }

    public InferencePipelineAggregationBuilder(StreamInput in, SetOnce<ModelLoadingService> modelLoadingService) throws IOException {
        super(in, NAME);
        modelId = in.readString();
        bucketPathMap = in.readMap(StreamInput::readString, StreamInput::readString);
        inferenceConfig = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
        gapPolicy = BucketHelpers.GapPolicy.readFrom(in);
        this.modelLoadingService = modelLoadingService;
    }

    void setModelId(String modelId) {
        this.modelId = modelId;
    }

    void setInferenceConfig(InferenceConfigUpdate inferenceConfig) {
        this.inferenceConfig = inferenceConfig;
    }

    void setGapPolicy(BucketHelpers.GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateHasParent(NAME, name);
        if (modelId == null) {
            context.addValidationError("Model Id must be set");
        }
        if (gapPolicy != BucketHelpers.GapPolicy.SKIP) {
            context.addValidationError("gap policy [" + gapPolicy + "] in not valid for [" + NAME + "] aggregation");
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeMap(bucketPathMap, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalNamedWriteable(inferenceConfig);
        gapPolicy.writeTo(out);
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {

        SetOnce<Model> model = new SetOnce<>();
        SetOnce<Exception> error = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Model> listener = new LatchedActionListener<>(
            ActionListener.wrap(model::set, error::set), latch);

        modelLoadingService.get().getModelForSearch(modelId, listener);
        try {
            // TODO Avoid the blocking wait
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Inference aggregation interrupted loading model", e);
        }

        Exception e = error.get();
        if (e != null) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            } else {
                throw new RuntimeException(error.get());
            }
        }

        InferenceConfigUpdate update = adaptForAggregation(inferenceConfig);

        return new InferencePipelineAggregator(name, bucketPathMap, metaData, update, model.get());
    }

    static InferenceConfigUpdate adaptForAggregation(InferenceConfigUpdate originalUpdate) {
        InferenceConfigUpdate updated;
        if (originalUpdate == null) {
            updated = new ResultsFieldUpdate(AGGREGATIONS_RESULTS_FIELD);
        } else {
            if (originalUpdate instanceof ClassificationConfigUpdate) {
                ClassificationConfigUpdate classUpdate = (ClassificationConfigUpdate)originalUpdate;

                // error if the top classes result field is set and not equal to the only acceptable value
                String topClassesField = classUpdate.getTopClassesResultsField();
                if (Strings.isNullOrEmpty(topClassesField) == false &&
                    ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD.equals(topClassesField) == false) {
                    throw ExceptionsHelper.badRequestException("setting option [{}] to [{}] is not valid for inference aggregations",
                        ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD, topClassesField);
                }
            }

            // error if the results field is set and not equal to the only acceptable value
            String resultsField = originalUpdate.getResultsField();
            if (Strings.isNullOrEmpty(resultsField) == false && AGGREGATIONS_RESULTS_FIELD.equals(resultsField) == false) {
                throw ExceptionsHelper.badRequestException("setting option [{}] to [{}] is not valid for inference aggregations",
                    ClassificationConfig.RESULTS_FIELD.getPreferredName(), resultsField);
            }

            // Create an update that changes the default results field.
            // This isn't necessary for top classes as the default is the same one used here
            updated = originalUpdate.newBuilder().setResultsField(AGGREGATIONS_RESULTS_FIELD).build();
        }

        return updated;
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
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketPathMap, modelId, inferenceConfig, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InferencePipelineAggregationBuilder other = (InferencePipelineAggregationBuilder) obj;
        return Objects.equals(bucketPathMap, other.bucketPathMap)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(gapPolicy, other.gapPolicy)
            && Objects.equals(inferenceConfig, other.inferenceConfig);
    }
}
