/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;

public class LearnToRankRescorerBuilder extends RescorerBuilder<LearnToRankRescorerBuilder> {

    public static final String NAME = "learn_to_rank";
    private static final ParseField MODEL_FIELD = new ParseField("model_id");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);

    static {
        PARSER.declareString(Builder::setModelId, MODEL_FIELD);
        PARSER.declareObject(Builder::setParams, (p, c) -> p.map(), PARAMS_FIELD);
    }

    public static LearnToRankRescorerBuilder fromXContent(
        XContentParser parser,
        ModelLoadingService modelLoadingService,
        ScriptService scriptService
    ) {
        return PARSER.apply(parser, null).build(modelLoadingService, scriptService);
    }

    private final String modelId;
    private final Map<String, Object> params;
    private final ScriptService scriptService;
    private final ModelLoadingService modelLoadingService;
    private final LocalModel localModel;
    private final LearnToRankConfig learnToRankConfig;
    private boolean rescoreOccurred = false;

    LearnToRankRescorerBuilder(
        String modelId,
        Map<String, Object> params,
        ModelLoadingService modelLoadingService,
        ScriptService scriptService
    ) {
        this.modelId = modelId;
        this.params = params;
        this.scriptService = scriptService;
        this.modelLoadingService = modelLoadingService;

        // Config and model will be set during successive rewrite phases.
        this.learnToRankConfig = null;
        this.localModel = null;
    }

    LearnToRankRescorerBuilder(String modelId, ModelLoadingService modelLoadingService, LearnToRankConfig learnToRankConfig) {
        this.modelId = modelId;
        this.modelLoadingService = modelLoadingService;
        this.learnToRankConfig = learnToRankConfig;

        // Local inference model is not loaded yet. Will be done in a later rewrite.
        this.localModel = null;

        // Templates has been applied already, so we do not need params and script service anymore.
        this.params = null;
        this.scriptService = null;
    }

    public LearnToRankRescorerBuilder(StreamInput input, ModelLoadingService modelLoadingService, ScriptService scriptService)
        throws IOException {
        super(input);
        this.modelId = input.readString();
        this.params = input.readMap();
        this.learnToRankConfig = input.readOptionalNamedWriteable(LearnToRankConfig.class);

        this.modelLoadingService = modelLoadingService;
        this.scriptService = scriptService;

        this.localModel = null;
    }

    LearnToRankRescorerBuilder(LearnToRankConfig learnToRankConfig, LocalModel localModel) {
        this.modelId = localModel.getModelId();
        this.learnToRankConfig = learnToRankConfig;
        this.localModel = localModel;

        // Model is loaded already, so we do not need the model loading service anymore.
        this.modelLoadingService = null;

        // Template has been applied already, so we do not need params and script service anymore.
        this.params = null;
        this.scriptService = null;
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

    public ModelLoadingService modelLoadingService() {
        return modelLoadingService;
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
            LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(modelId, modelLoadingService, rewrittenConfig);
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
                        for (LearnToRankFeatureExtractorBuilder builder : retrievedInferenceConfig.getFeatureExtractorBuilders()) {
                            builder.validate();
                        }
                        configSetOnce.set(applyParams(retrievedInferenceConfig, ctx));
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
        LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(modelId, modelLoadingService, null) {
            @Override
            public RescorerBuilder<LearnToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
                if (configSetOnce.get() == null) {
                    // Still waiting for the model to be loaded.
                    return this;
                }

                LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(modelId, modelLoadingService, configSetOnce.get());

                if (windowSize() != null) {
                    builder.windowSize(windowSize());
                }

                return builder;
            }
        };

        if (windowSize() != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    private LearnToRankConfig applyParams(LearnToRankConfig config, QueryRewriteContext ctx) throws IOException {
        if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) == false) {
            return config;
        }

        if (params == null || params.isEmpty()) {
            return config;
        }

        try (XContentBuilder configSourceBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            String templateSource = BytesReference.bytes(config.toXContent(configSourceBuilder, EMPTY_PARAMS)).utf8ToString();
            if (templateSource.contains("{{") == false) {
                return config;
            }
            Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, templateSource, Collections.emptyMap());
            String parsedTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(params).execute();

            XContentParser parser = XContentType.JSON.xContent().createParser(ctx.getParserConfig(), parsedTemplate);

            return LearnToRankConfig.fromXContentStrict(parser);
        }
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

        if (modelLoadingService == null) {
            throw new IllegalStateException("Model loading service must be available");
        }
        LearnToRankConfig rewrittenConfig = Rewriteable.rewrite(learnToRankConfig, ctx);
        SetOnce<LocalModel> localModelSetOnce = new SetOnce<>();
        ctx.registerAsyncAction((c, l) -> modelLoadingService.getModelForLearnToRank(modelId, ActionListener.wrap(lm -> {
            localModelSetOnce.set(lm);
            l.onResponse(null);
        }, l::onFailure)));

        LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(modelId, modelLoadingService, learnToRankConfig) {
            @Override
            public RescorerBuilder<LearnToRankRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
                if (localModelSetOnce.get() == null) {
                    // Still waiting for the model to be loaded.
                    return this;
                }

                LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(learnToRankConfig, localModelSetOnce.get());

                if (windowSize() != null) {
                    builder.windowSize(windowSize());
                }

                return builder;
            }
        };

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
        LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(rewrittenConfig, localModel);
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    @Override
    protected LearnToRankRescorerContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        rescoreOccurred = true;

        assert learnToRankConfig != null;
        assert localModel != null;

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
            && Objects.equals(modelLoadingService, that.modelLoadingService)
            && Objects.equals(scriptService, that.scriptService)
            && rescoreOccurred == that.rescoreOccurred;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            modelId,
            params,
            learnToRankConfig,
            localModel,
            modelLoadingService,
            scriptService,
            rescoreOccurred
        );
    }

    static class Builder {
        private String modelId;
        private Map<String, Object> params = Collections.emptyMap();

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        LearnToRankRescorerBuilder build(ModelLoadingService modelLoadingService, ScriptService scriptService) {
            return new LearnToRankRescorerBuilder(modelId, params, modelLoadingService, scriptService);
        }
    }
}
