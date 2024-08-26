/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class LearningToRankRescorerBuilder extends RescorerBuilder<LearningToRankRescorerBuilder> {

    public static final ParseField NAME = new ParseField("learning_to_rank");
    public static final ParseField MODEL_FIELD = new ParseField("model_id");
    public static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), false, Builder::new);

    static {
        PARSER.declareString(Builder::setModelId, MODEL_FIELD);
        PARSER.declareObject(Builder::setParams, (p, c) -> p.map(), PARAMS_FIELD);
        PARSER.declareRequiredFieldSet(MODEL_FIELD.getPreferredName());
    }

    public static LearningToRankRescorerBuilder fromXContent(XContentParser parser, LearningToRankService learningToRankService) {
        return PARSER.apply(parser, null).build(learningToRankService);
    }

    private final String modelId;
    private final Map<String, Object> params;
    private final LearningToRankService learningToRankService;
    private final LocalModel localModel;
    private final LearningToRankConfig learningToRankConfig;

    private boolean rescoreOccurred = false;

    LearningToRankRescorerBuilder(String modelId, Map<String, Object> params, LearningToRankService learningToRankService) {
        this(modelId, null, params, learningToRankService);
    }

    LearningToRankRescorerBuilder(
        String modelId,
        LearningToRankConfig learningToRankConfig,
        Map<String, Object> params,
        LearningToRankService learningToRankService
    ) {
        this.modelId = modelId;
        this.params = params;
        this.learningToRankConfig = learningToRankConfig;
        this.learningToRankService = learningToRankService;

        // Local inference model is not loaded yet. Will be done in a later rewrite.
        this.localModel = null;
    }

    LearningToRankRescorerBuilder(
        LocalModel localModel,
        LearningToRankConfig learningToRankConfig,
        Map<String, Object> params,
        LearningToRankService learningToRankService
    ) {
        this.modelId = localModel.getModelId();
        this.params = params;
        this.learningToRankConfig = learningToRankConfig;
        this.localModel = localModel;
        this.learningToRankService = learningToRankService;
    }

    public LearningToRankRescorerBuilder(StreamInput input, LearningToRankService learningToRankService) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.params = input.readGenericMap();
        this.learningToRankConfig = (LearningToRankConfig) input.readOptionalNamedWriteable(InferenceConfig.class);
        this.learningToRankService = learningToRankService;

        this.localModel = null;
    }

    public String modelId() {
        return modelId;
    }

    public Map<String, Object> params() {
        return params;
    }

    public LearningToRankConfig learningToRankConfig() {
        return learningToRankConfig;
    }

    public LearningToRankService learningToRankService() {
        return learningToRankService;
    }

    public LocalModel localModel() {
        return localModel;
    }

    @Override
    public RescorerBuilder<LearningToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.convertToDataRewriteContext() != null) {
            return doDataNodeRewrite(ctx);
        }
        if (ctx.convertToSearchExecutionContext() != null) {
            return doSearchRewrite(ctx);
        }
        return doCoordinatorNodeRewrite(ctx);
    }

    @Override
    public ActionRequestValidationException validate(SearchSourceBuilder source, ActionRequestValidationException validationException) {
        validationException = super.validate(source, validationException);

        int searchRequestPaginationSize = source.from() + source.size();

        if (windowSize() < searchRequestPaginationSize) {
            return addValidationError(
                "rescorer [window_size] is too small and should be at least the value of [from + size: "
                    + searchRequestPaginationSize
                    + "] but was ["
                    + windowSize()
                    + "]",
                validationException
            );
        }

        @SuppressWarnings("rawtypes")
        List<RescorerBuilder> rescorers = source.rescores();
        assert rescorers != null && rescorers.contains(this);

        for (int i = rescorers.indexOf(this) + 1; i < rescorers.size(); i++) {
            RescorerBuilder<?> nextRescorer = rescorers.get(i);
            int nextRescorerWindowSize = nextRescorer.windowSize() != null ? nextRescorer.windowSize() : DEFAULT_WINDOW_SIZE;
            if (windowSize() < nextRescorerWindowSize) {
                return addValidationError(
                    "unable to add a rescorer with [window_size: "
                        + nextRescorerWindowSize
                        + "] because a rescorer of type ["
                        + getWriteableName()
                        + "] with a smaller [window_size: "
                        + windowSize()
                        + "] has been added before",
                    validationException
                );
            }
        }

        return validationException;
    }

    /**
     * Here we fetch the stored model inference context, apply the given update, and rewrite.
     *
     * This can and be done on the coordinator as it not only validates if the stored model is of the appropriate type, it allows
     * any stored logic to rewrite on the coordinator level if possible.
     * @param ctx QueryRewriteContext
     * @return rewritten LearningToRankRescorerBuilder or self if no changes
     * @throws IOException when rewrite fails
     */
    private RescorerBuilder<LearningToRankRescorerBuilder> doCoordinatorNodeRewrite(QueryRewriteContext ctx) throws IOException {
        // We have requested for the stored config and fetch is completed, get the config and rewrite further if required
        if (learningToRankConfig != null) {
            LearningToRankConfig rewrittenConfig = Rewriteable.rewrite(learningToRankConfig, ctx);
            if (rewrittenConfig == learningToRankConfig) {
                return this;
            }
            LearningToRankRescorerBuilder builder = new LearningToRankRescorerBuilder(
                modelId,
                rewrittenConfig,
                params,
                learningToRankService
            );
            if (windowSize != null) {
                builder.windowSize(windowSize);
            }
            return builder;
        }

        if (learningToRankService == null) {
            throw new IllegalStateException("Learning to rank service must be available");
        }

        SetOnce<LearningToRankConfig> configSetOnce = new SetOnce<>();
        GetTrainedModelsAction.Request request = new GetTrainedModelsAction.Request(modelId);
        request.setAllowNoResources(false);
        ctx.registerAsyncAction(
            (c, l) -> learningToRankService.loadLearningToRankConfig(modelId, params, ActionListener.wrap(learningToRankConfig -> {
                configSetOnce.set(learningToRankConfig);
                l.onResponse(null);
            }, l::onFailure))
        );

        LearningToRankRescorerBuilder builder = new RewritingLearningToRankRescorerBuilder(
            (rewritingBuilder) -> configSetOnce.get() == null
                ? rewritingBuilder
                : new LearningToRankRescorerBuilder(modelId, configSetOnce.get(), params, learningToRankService)
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
     */
    private RescorerBuilder<LearningToRankRescorerBuilder> doDataNodeRewrite(QueryRewriteContext ctx) throws IOException {
        assert learningToRankConfig != null;

        // The model is already loaded, no need to rewrite further.
        if (localModel != null) {
            return this;
        }

        if (learningToRankService == null) {
            throw new IllegalStateException("Learning to rank service must be available");
        }

        LearningToRankConfig rewrittenConfig = Rewriteable.rewrite(learningToRankConfig, ctx);
        SetOnce<LocalModel> localModelSetOnce = new SetOnce<>();
        ctx.registerAsyncAction((c, l) -> learningToRankService.loadLocalModel(modelId, ActionListener.wrap(lm -> {
            localModelSetOnce.set(lm);
            l.onResponse(null);
        }, l::onFailure)));

        LearningToRankRescorerBuilder builder = new RewritingLearningToRankRescorerBuilder(
            (rewritingBuilder) -> localModelSetOnce.get() != null
                ? new LearningToRankRescorerBuilder(localModelSetOnce.get(), rewrittenConfig, params, learningToRankService)
                : rewritingBuilder
        );

        if (windowSize() != null) {
            builder.windowSize(windowSize());
        }
        return builder;
    }

    /**
     * This rewrite phase occurs on the data node when we know we will want to use the model for inference
     * @param ctx Rewrite context
     * @return A rewritten rescorer with a model definition or a model definition supplier populated
     * @throws IOException If fetching, parsing, or overall rewrite failures occur
     */
    private RescorerBuilder<LearningToRankRescorerBuilder> doSearchRewrite(QueryRewriteContext ctx) throws IOException {
        if (learningToRankConfig == null) {
            return this;
        }
        LearningToRankConfig rewrittenConfig = Rewriteable.rewrite(learningToRankConfig, ctx);
        if (rewrittenConfig == learningToRankConfig) {
            return this;
        }
        LearningToRankRescorerBuilder builder = new LearningToRankRescorerBuilder(
            localModel,
            rewrittenConfig,
            params,
            learningToRankService
        );
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    @Override
    protected LearningToRankRescorerContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        rescoreOccurred = true;
        return new LearningToRankRescorerContext(windowSize, LearningToRankRescorer.INSTANCE, learningToRankConfig, localModel, context);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.LTR_SERVERLESS_RELEASE;
    }

    @Override
    protected boolean isWindowSizeRequired() {
        return true;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        assert localModel == null || rescoreOccurred : "Unnecessarily populated local model object";
        out.writeString(modelId);
        out.writeGenericMap(params);
        out.writeOptionalNamedWriteable(learningToRankConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME.getPreferredName());
        builder.field(MODEL_FIELD.getPreferredName(), modelId);
        if (this.params != null) {
            builder.field(PARAMS_FIELD.getPreferredName(), this.params);
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        LearningToRankRescorerBuilder that = (LearningToRankRescorerBuilder) o;

        return Objects.equals(modelId, that.modelId)
            && Objects.equals(params, that.params)
            && Objects.equals(learningToRankConfig, that.learningToRankConfig)
            && Objects.equals(localModel, that.localModel)
            && Objects.equals(learningToRankService, that.learningToRankService)
            && rescoreOccurred == that.rescoreOccurred;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), modelId, params, learningToRankConfig, localModel, learningToRankService, rescoreOccurred);
    }

    static class Builder {
        private String modelId;
        private Map<String, Object> params = null;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        LearningToRankRescorerBuilder build(LearningToRankService learningToRankService) {
            return new LearningToRankRescorerBuilder(modelId, params, learningToRankService);
        }
    }

    private static class RewritingLearningToRankRescorerBuilder extends LearningToRankRescorerBuilder {

        private final Function<RewritingLearningToRankRescorerBuilder, LearningToRankRescorerBuilder> rewriteFunction;

        RewritingLearningToRankRescorerBuilder(
            Function<RewritingLearningToRankRescorerBuilder, LearningToRankRescorerBuilder> rewriteFunction
        ) {
            super(null, null, null);
            this.rewriteFunction = rewriteFunction;
        }

        @Override
        public RescorerBuilder<LearningToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
            LearningToRankRescorerBuilder builder = this.rewriteFunction.apply(this);

            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }

            return builder;
        }
    }
}
