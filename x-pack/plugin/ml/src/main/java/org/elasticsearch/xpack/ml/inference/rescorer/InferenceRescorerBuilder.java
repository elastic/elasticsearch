/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public class InferenceRescorerBuilder extends RescorerBuilder<InferenceRescorerBuilder> {

    public static final String NAME = "inference";
    private static final ParseField MODEL = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);
    static {
        PARSER.declareString(Builder::setModelId, MODEL);
        PARSER.declareNamedObject(
            Builder::setInferenceConfigUpdate,
            (p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, false),
            INFERENCE_CONFIG
        );
    }

    public static InferenceRescorerBuilder fromXContent(XContentParser parser, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        return PARSER.apply(parser, null).build(modelLoadingServiceSupplier);
    }

    private final String modelId;
    private final LearnToRankConfigUpdate inferenceConfigUpdate;
    // Only available on the data node
    private final LocalModel inferenceDefinition;
    private final LearnToRankConfig inferenceConfig;
    private final Supplier<LocalModel> inferenceDefinitionSupplier;
    private final Supplier<ModelLoadingService> modelLoadingServiceSupplier;
    private boolean rescoreOccurred;

    public InferenceRescorerBuilder(String modelId, LearnToRankConfigUpdate inferenceConfigUpdate, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceConfigUpdate = inferenceConfigUpdate;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = null;
        this.inferenceConfig = null;
    }

    InferenceRescorerBuilder(String modelId, LearnToRankConfigUpdate update, LearnToRankConfig inferenceConfig, LocalModel inferenceDefinition) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceConfigUpdate = update;
        this.inferenceDefinition = Objects.requireNonNull(inferenceDefinition);
        this.inferenceDefinitionSupplier = null;
        this.modelLoadingServiceSupplier = null;
        this.inferenceConfig = inferenceConfig;
    }

    private InferenceRescorerBuilder(
        String modelId,
        LearnToRankConfigUpdate inferenceConfigUpdate,
        Supplier<ModelLoadingService> modelLoadingServiceSupplier,
        Supplier<LocalModel> inferenceDefinitionSupplier
    ) {
        this.modelId = modelId;
        this.inferenceConfigUpdate = inferenceConfigUpdate;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = inferenceDefinitionSupplier;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceConfig = null;
    }

    public InferenceRescorerBuilder(StreamInput input, Supplier<ModelLoadingService> modelLoadingServiceSupplier) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.inferenceConfigUpdate = input.readOptionalWriteable(LearnToRankConfigUpdate::new);
        this.inferenceDefinitionSupplier = null;
        this.inferenceDefinition = null;
        this.inferenceConfig = null;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * should be updated once {@link InferenceRescorerFeature} is removed
     */
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO: update transport version when released!
        return TransportVersion.current();
    }

    @Override
    public RescorerBuilder<InferenceRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (inferenceDefinitionSupplier != null && inferenceDefinitionSupplier.get() == null) {
            // awaiting fetch
            return this;
        }
        LearnToRankConfigUpdate rewrittenUpdate = inferenceConfigUpdate == null ? null : Rewriteable.rewrite(inferenceConfigUpdate, ctx);
        boolean rewritten = rewrittenUpdate != inferenceConfigUpdate;
        if (inferenceDefinition != null) {
            return this;
        }
        if (inferenceDefinitionSupplier != null) {
            LocalModel inferenceDefinition = inferenceDefinitionSupplier.get();
            LearnToRankConfig storedInferenceConfig = (LearnToRankConfig) inferenceDefinition.getInferenceConfig();
            storedInferenceConfig = rewrittenUpdate == null ? storedInferenceConfig : rewrittenUpdate.apply(inferenceConfig);
            // throw on async tasks here to protect users from making strange requests
            storedInferenceConfig = Rewriteable.rewrite(storedInferenceConfig, ctx, true);
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(modelId, rewrittenUpdate, storedInferenceConfig, inferenceDefinition);
            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }
            return builder;
        }
        // We don't want to load the model on the coordinator
        if (ctx.convertToDataRewriteContext() != null) {
            if (modelLoadingServiceSupplier == null || modelLoadingServiceSupplier.get() == null) {
                throw new IllegalStateException("Model loading service must be available");
            }
            SetOnce<LocalModel> inferenceDefinitionSetOnce = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> modelLoadingServiceSupplier.get().getModelForLearnToRank(modelId, ActionListener.wrap(lm -> {
                // Ensure all feature extractors are fully parsed and ready to use
                if (lm.getInferenceConfig() instanceof LearnToRankConfig learnToRankConfig) {
                    for (LearnToRankFeatureExtractorBuilder builder : learnToRankConfig.getFeatureExtractorBuilders()) {
                        builder.validate();
                    }
                }
                inferenceDefinitionSetOnce.set(lm);
                l.onResponse(null);
            }, l::onFailure)));
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
                modelId,
                rewrittenUpdate,
                modelLoadingServiceSupplier,
                inferenceDefinitionSetOnce::get
            );
            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }
            return builder;
        }
        if (rewritten) {
            return new InferenceRescorerBuilder(modelId, rewrittenUpdate, modelLoadingServiceSupplier);
        }
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceDefinitionSupplier != null) {
            throw new IllegalStateException("supplier must be null, missing a rewriteAndFetch?");
        }
        assert inferenceDefinition == null || rescoreOccurred : "Unnecessarily populated local model object";
        out.writeString(modelId);
        out.writeOptionalNamedWriteable(inferenceConfigUpdate);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(MODEL.getPreferredName(), modelId);
        if (inferenceConfigUpdate != null) {
            NamedXContentObjectHelper.writeNamedObject(builder, params, INFERENCE_CONFIG.getPreferredName(), inferenceConfigUpdate);
        }
        builder.endObject();
    }

    @Override
    protected InferenceRescorerContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        rescoreOccurred = true;
        return new InferenceRescorerContext(windowSize, InferenceRescorer.INSTANCE, inferenceConfig, inferenceDefinition, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InferenceRescorerBuilder that = (InferenceRescorerBuilder) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(inferenceDefinition, that.inferenceDefinition)
            && Objects.equals(inferenceConfigUpdate, that.inferenceConfigUpdate)
            && Objects.equals(inferenceDefinitionSupplier, that.inferenceDefinitionSupplier)
            && Objects.equals(modelLoadingServiceSupplier, that.modelLoadingServiceSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), modelId, inferenceConfigUpdate, inferenceDefinition, inferenceDefinitionSupplier, modelLoadingServiceSupplier);
    }

    LearnToRankConfigUpdate getInferenceConfigUpdate() {
        return inferenceConfigUpdate;
    }

    static class Builder {
        private String modelId;
        private LearnToRankConfigUpdate inferenceConfigUpdate;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setInferenceConfigUpdate(InferenceConfigUpdate inferenceConfigUpdate) {
            if (inferenceConfigUpdate instanceof LearnToRankConfigUpdate learnToRankConfigUpdate) {
                this.inferenceConfigUpdate = learnToRankConfigUpdate;
                return;
            }
            throw new IllegalArgumentException(
                Strings.format(
                    "[%s] only allows a [%s] object to be configured",
                    INFERENCE_CONFIG.getPreferredName(),
                    LearnToRankConfigUpdate.NAME.getPreferredName()
                )
            );
        }

        InferenceRescorerBuilder build(Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
            return new InferenceRescorerBuilder(modelId, inferenceConfigUpdate, modelLoadingServiceSupplier);
        }
    }
}
