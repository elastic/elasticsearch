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
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class InferenceRescorerBuilder extends RescorerBuilder<InferenceRescorerBuilder> {

    public static final String NAME = "inference";
    private static final ParseField MODEL = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    private static final ParseField INTERNAL_INFERENCE_CONFIG = new ParseField("_internal_inference_config");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);
    static {
        PARSER.declareString(Builder::setModelId, MODEL);
        PARSER.declareNamedObject(
            Builder::setInferenceConfigUpdate,
            (p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, false),
            INFERENCE_CONFIG
        );
        PARSER.declareNamedObject(
            Builder::setInferenceConfig,
            (p, c, name) -> p.namedObject(StrictlyParsedInferenceConfig.class, name, false),
            INTERNAL_INFERENCE_CONFIG
        );
    }

    public static InferenceRescorerBuilder fromXContent(XContentParser parser, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        return PARSER.apply(parser, null).build(modelLoadingServiceSupplier);
    }

    private final String modelId;
    private final LearnToRankConfigUpdate inferenceConfigUpdate;
    private final LearnToRankConfig inferenceConfig;
    private final LocalModel inferenceDefinition;
    private final Supplier<LocalModel> inferenceDefinitionSupplier;
    private final Supplier<ModelLoadingService> modelLoadingServiceSupplier;
    private final Supplier<LearnToRankConfig> inferenceConfigSupplier;
    private boolean rescoreOccurred;

    public InferenceRescorerBuilder(
        String modelId,
        LearnToRankConfigUpdate inferenceConfigUpdate,
        Supplier<ModelLoadingService> modelLoadingServiceSupplier
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceConfigUpdate = inferenceConfigUpdate;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = null;
        this.inferenceConfigSupplier = null;
        this.inferenceConfig = null;
    }

    InferenceRescorerBuilder(String modelId, LearnToRankConfig inferenceConfig, Supplier<ModelLoadingService> modelLoadingServiceSupplier) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceConfigUpdate = null;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = null;
        this.inferenceConfigSupplier = null;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceConfig = Objects.requireNonNull(inferenceConfig);
    }

    private InferenceRescorerBuilder(
        String modelId,
        LearnToRankConfigUpdate update,
        Supplier<ModelLoadingService> modelLoadingServiceSupplier,
        Supplier<LearnToRankConfig> inferenceConfigSupplier
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.inferenceConfigUpdate = update;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = null;
        this.inferenceConfigSupplier = inferenceConfigSupplier;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceConfig = null;
    }

    private InferenceRescorerBuilder(
        String modelId,
        LearnToRankConfig inferenceConfig,
        Supplier<ModelLoadingService> modelLoadingServiceSupplier,
        Supplier<LocalModel> inferenceDefinitionSupplier
    ) {
        this.modelId = modelId;
        this.inferenceConfigUpdate = null;
        this.inferenceDefinition = null;
        this.inferenceDefinitionSupplier = inferenceDefinitionSupplier;
        this.modelLoadingServiceSupplier = modelLoadingServiceSupplier;
        this.inferenceConfigSupplier = null;
        this.inferenceConfig = inferenceConfig;
    }

    InferenceRescorerBuilder(String modelId, LearnToRankConfig inferenceConfig, LocalModel inferenceDefinition) {
        this.modelId = modelId;
        this.inferenceConfigUpdate = null;
        this.inferenceDefinition = inferenceDefinition;
        this.inferenceDefinitionSupplier = null;
        this.modelLoadingServiceSupplier = null;
        this.inferenceConfigSupplier = null;
        this.inferenceConfig = inferenceConfig;
    }

    public InferenceRescorerBuilder(StreamInput input, Supplier<ModelLoadingService> modelLoadingServiceSupplier) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.inferenceConfigUpdate = (LearnToRankConfigUpdate) input.readOptionalNamedWriteable(InferenceConfigUpdate.class);
        this.inferenceDefinitionSupplier = null;
        this.inferenceConfigSupplier = null;
        this.inferenceDefinition = null;
        this.inferenceConfig = (LearnToRankConfig) input.readOptionalNamedWriteable(InferenceConfig.class);
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

    /**
     * Here we fetch the stored model inference context, apply the given update, and rewrite.
     *
     * This can and should be done on the coordinator as it not only validates if the stored model is of the appropriate type, it allows
     * any stored logic to rewrite on the coordinator level if possible.
     * @param ctx QueryRewriteContext on the coordinator
     * @return rewritten InferenceRescorerBuilder or self if no changes
     * @throws IOException when rewrite fails
     */
    private RescorerBuilder<InferenceRescorerBuilder> doCoordinatorRewrite(QueryRewriteContext ctx) throws IOException {
        // Awaiting fetch
        if (inferenceConfigSupplier != null && inferenceConfigSupplier.get() == null) {
            return this;
        }
        // Already have the inference config, complete any additional rewriting required
        if (inferenceConfig != null) {
            LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(inferenceConfig, ctx);
            if (rewrittenConfig == inferenceConfig) {
                return this;
            }
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(modelId, rewrittenConfig, modelLoadingServiceSupplier);
            if (windowSize != null) {
                builder.windowSize(windowSize);
            }
            return builder;
        }
        // We have requested for the stored config and fetch is completed, get the config and rewrite further if required
        if (inferenceConfigSupplier != null) {
            LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(inferenceConfigSupplier.get(), ctx);
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(modelId, rewrittenConfig, modelLoadingServiceSupplier);
            if (windowSize != null) {
                builder.windowSize(windowSize);
            }
            return builder;
        }
        SetOnce<LearnToRankConfig> configSetOnce = new SetOnce<>();
        GetTrainedModelsAction.Request request = new GetTrainedModelsAction.Request(modelId);
        request.setAllowNoResources(false);
        ctx.registerAsyncAction(
            (c, l) -> ClientHelper.executeAsyncWithOrigin(
                c,
                ClientHelper.ML_ORIGIN,
                GetTrainedModelsAction.INSTANCE,
                request,
                ActionListener.wrap(trainedModels -> {
                    TrainedModelConfig config = trainedModels.getResources().results().get(0);
                    if (config.getInferenceConfig() instanceof LearnToRankConfig retrievedInferenceConfig) {
                        retrievedInferenceConfig = inferenceConfigUpdate == null
                            ? retrievedInferenceConfig
                            : inferenceConfigUpdate.apply(retrievedInferenceConfig);
                        for (LearnToRankFeatureExtractorBuilder builder : retrievedInferenceConfig.getFeatureExtractorBuilders()) {
                            builder.validate();
                        }
                        configSetOnce.set(retrievedInferenceConfig);
                        l.onResponse(null);
                        return;
                    }
                    l.onFailure(
                        ExceptionsHelper.badRequestException(
                            Messages.getMessage(
                                Messages.INFERENCE_CONFIG_INCORRECT_TYPE,
                                Optional.ofNullable(config.getInferenceConfig()).map(InferenceConfig::getName).orElse("null"),
                                LearnToRankConfig.NAME.getPreferredName()
                            )
                        )
                    );
                }, l::onFailure)
            )
        );
        InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
            modelId,
            inferenceConfigUpdate,
            modelLoadingServiceSupplier,
            configSetOnce::get
        );
        if (windowSize() != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    /**
     * This rewrite phase occurs on the data node when we know we will want to use the model for inference
     * @param ctx Rewrite context
     * @return A rewritten rescorer with a model definition or a model definition supplier populated
     * @throws IOException If fetching, parsing, or overall rewrite failures occur
     */
    private RescorerBuilder<InferenceRescorerBuilder> doDataNodeRewrite(QueryRewriteContext ctx) throws IOException {
        assert inferenceConfig != null;
        // We already have an inference definition, no need to do any rewriting
        if (inferenceDefinition != null) {
            return this;
        }
        // Awaiting fetch
        if (inferenceDefinitionSupplier != null && inferenceDefinitionSupplier.get() == null) {
            return this;
        }
        if (inferenceDefinitionSupplier != null) {
            LocalModel inferenceDefinition = inferenceDefinitionSupplier.get();
            InferenceRescorerBuilder builder = new InferenceRescorerBuilder(modelId, inferenceConfig, inferenceDefinition);
            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }
            return builder;
        }
        if (modelLoadingServiceSupplier == null || modelLoadingServiceSupplier.get() == null) {
            throw new IllegalStateException("Model loading service must be available");
        }
        SetOnce<LocalModel> inferenceDefinitionSetOnce = new SetOnce<>();
        ctx.registerAsyncAction((c, l) -> modelLoadingServiceSupplier.get().getModelForLearnToRank(modelId, ActionListener.wrap(lm -> {
            inferenceDefinitionSetOnce.set(lm);
            l.onResponse(null);
        }, l::onFailure)));
        InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
            modelId,
            inferenceConfig,
            modelLoadingServiceSupplier,
            inferenceDefinitionSetOnce::get
        );
        if (windowSize() != null) {
            builder.windowSize(windowSize());
        }
        return builder;
    }

    @Override
    public RescorerBuilder<InferenceRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.convertToCoordinatorRewriteContext() != null) {
            return doCoordinatorRewrite(ctx);
        }
        if (ctx.convertToDataRewriteContext() != null) {
            return doDataNodeRewrite(ctx);
        }
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    LearnToRankConfig getInferenceConfig() {
        return inferenceConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceDefinitionSupplier != null || inferenceConfigSupplier != null) {
            throw new IllegalStateException("suppliers must be null, missing a rewriteAndFetch?");
        }
        assert inferenceDefinition == null || rescoreOccurred : "Unnecessarily populated local model object";
        out.writeString(modelId);
        out.writeOptionalNamedWriteable(inferenceConfigUpdate);
        out.writeOptionalNamedWriteable(inferenceConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(MODEL.getPreferredName(), modelId);
        if (inferenceConfigUpdate != null) {
            NamedXContentObjectHelper.writeNamedObject(builder, params, INFERENCE_CONFIG.getPreferredName(), inferenceConfigUpdate);
        }
        if (inferenceConfig != null) {
            NamedXContentObjectHelper.writeNamedObject(builder, params, INTERNAL_INFERENCE_CONFIG.getPreferredName(), inferenceConfig);
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
            && Objects.equals(inferenceConfig, that.inferenceConfig)
            && Objects.equals(inferenceDefinitionSupplier, that.inferenceDefinitionSupplier)
            && Objects.equals(modelLoadingServiceSupplier, that.modelLoadingServiceSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            modelId,
            inferenceConfigUpdate,
            inferenceConfig,
            inferenceDefinition,
            inferenceDefinitionSupplier,
            modelLoadingServiceSupplier
        );
    }

    LearnToRankConfigUpdate getInferenceConfigUpdate() {
        return inferenceConfigUpdate;
    }

    static class Builder {
        private String modelId;
        private LearnToRankConfigUpdate inferenceConfigUpdate;
        private LearnToRankConfig inferenceConfig;

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

        void setInferenceConfig(InferenceConfig inferenceConfig) {
            if (inferenceConfig instanceof LearnToRankConfig learnToRankConfig) {
                this.inferenceConfig = learnToRankConfig;
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
            assert inferenceConfig == null || inferenceConfigUpdate == null;
            if (inferenceConfig != null) {
                return new InferenceRescorerBuilder(modelId, inferenceConfig, modelLoadingServiceSupplier);
            } else {
                return new InferenceRescorerBuilder(modelId, inferenceConfigUpdate, modelLoadingServiceSupplier);
            }
        }
    }
}
