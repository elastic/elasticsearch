/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class LearnToRankRescorerBuilder extends RescorerBuilder<LearnToRankRescorerBuilder> {

    public static final String NAME = "learn_to_rank";
    private static final ParseField MODEL_FIELD = new ParseField("model_id");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);

    static {
        PARSER.declareString(Builder::setModelId, MODEL_FIELD);
        PARSER.declareObject(Builder::setParams, (p, c) -> p.map(), PARAMS_FIELD);
    }

    public static LearnToRankRescorerBuilder fromXContent(XContentParser parser, LearnToRankService learnToRankService) {
        return PARSER.apply(parser, null).build(learnToRankService);
    }

    private final String modelId;
    private final Map<String, Object> params;
    private final LearnToRankService learnToRankService;
    private final LocalModel localModel;
    private final LearnToRankConfig learnToRankConfig;

    private boolean rescoreOccurred = false;

    LearnToRankRescorerBuilder(String modelId, Map<String, Object> params, LearnToRankService learnToRankService) {
        this(modelId, null, params, learnToRankService);
    }

    LearnToRankRescorerBuilder(
        String modelId,
        LearnToRankConfig learnToRankConfig,
        Map<String, Object> params,
        LearnToRankService learnToRankService
    ) {
        this.modelId = modelId;
        this.params = params;
        this.learnToRankConfig = learnToRankConfig;
        this.learnToRankService = learnToRankService;

        // Local inference model is not loaded yet. Will be done in a later rewrite.
        this.localModel = null;
    }

    LearnToRankRescorerBuilder(
        LocalModel localModel,
        LearnToRankConfig learnToRankConfig,
        Map<String, Object> params,
        LearnToRankService learnToRankService
    ) {
        this.modelId = localModel.getModelId();
        this.params = params;
        this.learnToRankConfig = learnToRankConfig;
        this.localModel = localModel;
        this.learnToRankService = learnToRankService;
    }

    public LearnToRankRescorerBuilder(StreamInput input, LearnToRankService learnToRankService) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.params = input.readMap();
        this.learnToRankConfig = (LearnToRankConfig) input.readOptionalNamedWriteable(InferenceConfig.class);
        this.learnToRankService = learnToRankService;

        this.localModel = null;
    }

    public String modelId() {
        return modelId;
    }

    public Map<String, Object> params() {
        return params;
    }

    public LearnToRankConfig learnToRankConfig() {
        return learnToRankConfig;
    }

    public LearnToRankService learnToRankService() {
        return learnToRankService;
    }

    public LocalModel localModel() {
        return localModel;
    }

    @Override
    public RescorerBuilder<LearnToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.convertToDataRewriteContext() != null) {
            return doDataNodeRewrite(ctx);
        }
        if (ctx.convertToSearchExecutionContext() != null) {
            return doSearchRewrite(ctx);
        }
        return doCoordinatorNodeRewrite(ctx);
    }

    /**
     * Here we fetch the stored model inference context, apply the given update, and rewrite.
     *
     * This can and be done on the coordinator as it not only validates if the stored model is of the appropriate type, it allows
     * any stored logic to rewrite on the coordinator level if possible.
     * @param ctx QueryRewriteContext
     * @return rewritten LearnToRankRescorerBuilder or self if no changes
     * @throws IOException when rewrite fails
     */
    private RescorerBuilder<LearnToRankRescorerBuilder> doCoordinatorNodeRewrite(QueryRewriteContext ctx) throws IOException {
        // We have requested for the stored config and fetch is completed, get the config and rewrite further if required
        if (learnToRankConfig != null) {
            LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(learnToRankConfig, ctx);
            if (rewrittenConfig == learnToRankConfig) {
                return this;
            }
            LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(modelId, rewrittenConfig, params, learnToRankService);
            if (windowSize != null) {
                builder.windowSize(windowSize);
            }
            return builder;
        }

        if (learnToRankService == null) {
            throw new IllegalStateException("Learn to rank service must be available");
        }

        SetOnce<LearnToRankConfig> configSetOnce = new SetOnce<>();
        GetTrainedModelsAction.Request request = new GetTrainedModelsAction.Request(modelId);
        request.setAllowNoResources(false);
        ctx.registerAsyncAction(
            (c, l) -> learnToRankService.loadLearnToRankConfig(modelId, params, ActionListener.wrap(learnToRankConfig -> {
                configSetOnce.set(learnToRankConfig);
                l.onResponse(null);
            }, l::onFailure))
        );

        LearnToRankRescorerBuilder builder = new RewritingLearnToRankRescorerBuilder(
            (rewritingBuilder) -> configSetOnce.get() == null
                ? rewritingBuilder
                : new LearnToRankRescorerBuilder(modelId, configSetOnce.get(), params, learnToRankService)
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
    private RescorerBuilder<LearnToRankRescorerBuilder> doDataNodeRewrite(QueryRewriteContext ctx) throws IOException {
        assert learnToRankConfig != null;

        // The model is already loaded, no need to rewrite further.
        if (localModel != null) {
            return this;
        }

        if (learnToRankService == null) {
            throw new IllegalStateException("Learn to rank service must be available");
        }

        LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(learnToRankConfig, ctx);
        SetOnce<LocalModel> localModelSetOnce = new SetOnce<>();
        ctx.registerAsyncAction((c, l) -> learnToRankService.loadLocalModel(modelId, ActionListener.wrap(lm -> {
            localModelSetOnce.set(lm);
            l.onResponse(null);
        }, l::onFailure)));

        LearnToRankRescorerBuilder builder = new RewritingLearnToRankRescorerBuilder(
            (rewritingBuilder) -> localModelSetOnce.get() != null
                ? new LearnToRankRescorerBuilder(localModelSetOnce.get(), rewrittenConfig, params, learnToRankService)
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
    private RescorerBuilder<LearnToRankRescorerBuilder> doSearchRewrite(QueryRewriteContext ctx) throws IOException {
        if (learnToRankConfig == null) {
            return this;
        }
        LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(learnToRankConfig, ctx);
        if (rewrittenConfig == learnToRankConfig) {
            return this;
        }
        LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(localModel, rewrittenConfig, params, learnToRankService);
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    @Override
    protected LearnToRankRescorerContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        rescoreOccurred = true;
        return new LearnToRankRescorerContext(windowSize, LearnToRankRescorer.INSTANCE, learnToRankConfig, localModel, context);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO: update transport version when released!
        return TransportVersion.current();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        assert localModel == null || rescoreOccurred : "Unnecessarily populated local model object";
        out.writeString(modelId);
        out.writeGenericMap(params);
        out.writeOptionalNamedWriteable(learnToRankConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
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
        LearnToRankRescorerBuilder that = (LearnToRankRescorerBuilder) o;

        return Objects.equals(modelId, that.modelId)
            && Objects.equals(params, that.params)
            && Objects.equals(learnToRankConfig, that.learnToRankConfig)
            && Objects.equals(localModel, that.localModel)
            && Objects.equals(learnToRankService, that.learnToRankService)
            && rescoreOccurred == that.rescoreOccurred;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), modelId, params, learnToRankConfig, localModel, learnToRankService, rescoreOccurred);
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

        LearnToRankRescorerBuilder build(LearnToRankService learnToRankService) {
            return new LearnToRankRescorerBuilder(modelId, params, learnToRankService);
        }
    }

    private static class RewritingLearnToRankRescorerBuilder extends LearnToRankRescorerBuilder {

        private final Function<RewritingLearnToRankRescorerBuilder, LearnToRankRescorerBuilder> rewriteFunction;

        RewritingLearnToRankRescorerBuilder(Function<RewritingLearnToRankRescorerBuilder, LearnToRankRescorerBuilder> rewriteFunction) {
            super(null, null, null);
            this.rewriteFunction = rewriteFunction;
        }

        @Override
        public RescorerBuilder<LearnToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
            LearnToRankRescorerBuilder builder = this.rewriteFunction.apply(this);

            if (windowSize() != null) {
                builder.windowSize(windowSize());
            }

            return builder;
        }
    }
}
