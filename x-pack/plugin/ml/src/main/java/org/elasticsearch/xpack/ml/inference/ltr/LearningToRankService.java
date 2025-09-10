/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheInvalidParameterException;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearningToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Map.entry;
import static org.elasticsearch.common.xcontent.XContentHelper.mergeDefaults;
import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_CONFIG_QUERY_BAD_FORMAT;

public class LearningToRankService {
    private static final Map<String, String> SCRIPT_OPTIONS = Map.ofEntries(
        entry(MustacheScriptEngine.DETECT_MISSING_PARAMS_OPTION, Boolean.TRUE.toString())
    );
    private final ModelLoadingService modelLoadingService;
    private final TrainedModelProvider trainedModelProvider;
    private final ScriptService scriptService;
    private final XContentParserConfiguration parserConfiguration;

    public LearningToRankService(
        ModelLoadingService modelLoadingService,
        TrainedModelProvider trainedModelProvider,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry
    ) {
        this(modelLoadingService, trainedModelProvider, scriptService, XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry));
    }

    LearningToRankService(
        ModelLoadingService modelLoadingService,
        TrainedModelProvider trainedModelProvider,
        ScriptService scriptService,
        XContentParserConfiguration parserConfiguration
    ) {
        this.modelLoadingService = modelLoadingService;
        this.scriptService = scriptService;
        this.trainedModelProvider = trainedModelProvider;
        this.parserConfiguration = parserConfiguration;
    }

    /**
     * Asynchronously load a regression model to be used for learning to rank.
     *
     * @param modelId The model id to be loaded.
     * @param listener Response listener.
     */
    public void loadLocalModel(String modelId, ActionListener<LocalModel> listener) {
        modelLoadingService.getModelForLearningToRank(modelId, listener);
    }

    /**
     * Asynchronously load the learning to rank config by model id.
     * Once the model is loaded, templates are executed using params provided.
     *
     * @param modelId Id of the model.
     * @param params Templates params.
     * @param listener Response listener.
     */
    public void loadLearningToRankConfig(String modelId, Map<String, Object> params, ActionListener<LearningToRankConfig> listener) {
        trainedModelProvider.getTrainedModel(
            modelLoadingService.getModelId(modelId),
            GetTrainedModelsAction.Includes.all(),
            null,
            ActionListener.wrap(trainedModelConfig -> {
                if (trainedModelConfig.getInferenceConfig() instanceof LearningToRankConfig retrievedInferenceConfig) {
                    listener.onResponse(applyParams(retrievedInferenceConfig, params));
                    return;
                }
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        Messages.getMessage(
                            Messages.INFERENCE_CONFIG_INCORRECT_TYPE,
                            Optional.ofNullable(trainedModelConfig.getInferenceConfig()).map(InferenceConfig::getName).orElse("null"),
                            LearningToRankConfig.NAME.getPreferredName()
                        )
                    )
                );
            }, listener::onFailure)
        );
    }

    /**
     * Applies templates params to a {@link LearningToRankConfig} object.
     *
     * @param config Original config.
     * @param params Templates params.
     * @return A {@link LearningToRankConfig} object with templates applied.
     *
     * @throws IOException
     */
    private LearningToRankConfig applyParams(LearningToRankConfig config, Map<String, Object> params) throws Exception {
        if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) == false) {
            return config;
        }

        List<LearningToRankFeatureExtractorBuilder> featureExtractorBuilders = new ArrayList<>();

        Map<String, Object> mergedParams = new HashMap<>(Objects.requireNonNullElse(params, Map.of()));
        mergeDefaults(mergedParams, config.getParamsDefaults());

        for (LearningToRankFeatureExtractorBuilder featureExtractorBuilder : config.getFeatureExtractorBuilders()) {
            featureExtractorBuilders.add(applyParams(featureExtractorBuilder, mergedParams));
        }

        return LearningToRankConfig.builder(config).setLearningToRankFeatureExtractorBuilders(featureExtractorBuilders).build();
    }

    /**
     * Applies templates to features extractors.
     *
     * @param featureExtractorBuilder Source feature extractor builder.
     * @param params Templates params.
     * @return A new feature extractor with templates applied.
     *
     * @throws IOException
     */
    private LearningToRankFeatureExtractorBuilder applyParams(
        LearningToRankFeatureExtractorBuilder featureExtractorBuilder,
        Map<String, Object> params
    ) throws Exception {
        if (featureExtractorBuilder instanceof QueryExtractorBuilder queryExtractorBuilder) {
            featureExtractorBuilder = applyParams(queryExtractorBuilder, params);
        }

        featureExtractorBuilder.validate();

        return featureExtractorBuilder;
    }

    /**
     * Applies templates to a {@link QueryExtractorBuilder} object.
     *
     * @param queryExtractorBuilder Source query extractor builder.
     * @param params Templates params.
     * @return A {@link QueryExtractorBuilder} with templates applied.
     *
     * @throws IOException
     */
    private QueryExtractorBuilder applyParams(QueryExtractorBuilder queryExtractorBuilder, Map<String, Object> params) throws IOException {
        String templateSource = templateSource(queryExtractorBuilder.query());

        if (templateSource.contains("{{") == false) {
            return queryExtractorBuilder;
        }

        try {
            Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, templateSource, SCRIPT_OPTIONS, Collections.emptyMap());
            String parsedTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(params).execute();
            try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfiguration, parsedTemplate)) {
                return new QueryExtractorBuilder(
                    queryExtractorBuilder.featureName(),
                    QueryProvider.fromXContent(parser, false, INFERENCE_CONFIG_QUERY_BAD_FORMAT),
                    queryExtractorBuilder.defaultScore()
                );
            }
        } catch (GeneralScriptException e) {
            if (e.getRootCause().getClass().getName().equals(MustacheInvalidParameterException.class.getName())) {
                // Can't use instanceof since it return unexpected result.
                return new QueryExtractorBuilder(
                    queryExtractorBuilder.featureName(),
                    defaultQuery(queryExtractorBuilder.defaultScore()),
                    queryExtractorBuilder.defaultScore()
                );
            }
            throw e;
        }
    }

    private String templateSource(QueryProvider queryProvider) throws IOException {
        try (XContentBuilder configSourceBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            return BytesReference.bytes(queryProvider.toXContent(configSourceBuilder, EMPTY_PARAMS)).utf8ToString();
        }
    }

    private QueryProvider defaultQuery(float score) throws IOException {
        QueryBuilder query = score == 0 ? new MatchNoneQueryBuilder() : new MatchAllQueryBuilder().boost(score);
        return QueryProvider.fromParsedQuery(query);
    }
}
