/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_CONFIG_QUERY_BAD_FORMAT;

public class LearnToRankService {
    private final ModelLoadingService modelLoadingService;
    private final TrainedModelProvider trainedModelProvider;
    private final ScriptService scriptService;
    private final XContentParserConfiguration parserConfiguration;

    public LearnToRankService(
        ModelLoadingService modelLoadingService,
        TrainedModelProvider trainedModelProvider,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry
    ) {
        this(modelLoadingService, trainedModelProvider, scriptService, XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry));
    }

    LearnToRankService(
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

    public void loadLocalModel(String modelId, ActionListener<LocalModel> listener) {
        modelLoadingService.getModelForLearnToRank(modelId, listener);
    }

    public void loadLearnToRankConfig(String modelId, Map<String, Object> params, ActionListener<LearnToRankConfig> listener) {
        trainedModelProvider.getTrainedModel(
            modelId,
            GetTrainedModelsAction.Includes.all(),
            null,
            ActionListener.wrap(trainedModelConfig -> {
                if (trainedModelConfig.getInferenceConfig() instanceof LearnToRankConfig retrievedInferenceConfig) {
                    for (LearnToRankFeatureExtractorBuilder builder : retrievedInferenceConfig.getFeatureExtractorBuilders()) {
                        builder.validate();
                    }
                    listener.onResponse(applyParams(retrievedInferenceConfig, params));
                    return;
                }
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        Messages.getMessage(
                            Messages.INFERENCE_CONFIG_INCORRECT_TYPE,
                            Optional.ofNullable(trainedModelConfig.getInferenceConfig()).map(InferenceConfig::getName).orElse("null"),
                            LearnToRankConfig.NAME.getPreferredName()
                        )
                    )
                );
            }, listener::onFailure)
        );
    }

    private LearnToRankConfig applyParams(LearnToRankConfig config, Map<String, Object> params) throws IOException {
        if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) == false) {
            return config;
        }

        if (params == null || params.isEmpty()) {
            // TODO: better handling of missing parameters.
            return config;
        }

        List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilders = new ArrayList<>();

        for (LearnToRankFeatureExtractorBuilder featureExtractorBuilder : config.getFeatureExtractorBuilders()) {
            featureExtractorBuilders.add(applyParams(featureExtractorBuilder, params));
        }

        return LearnToRankConfig.builder(config).setLearnToRankFeatureExtractorBuilders(featureExtractorBuilders).build();
    }

    private LearnToRankFeatureExtractorBuilder applyParams(
        LearnToRankFeatureExtractorBuilder featureExtractorBuilder,
        Map<String, Object> params
    ) throws IOException {
        if (featureExtractorBuilder instanceof QueryExtractorBuilder queryExtractorBuilder) {
            return applyParams(queryExtractorBuilder, params);
        }

        return featureExtractorBuilder;
    }

    private QueryExtractorBuilder applyParams(QueryExtractorBuilder queryExtractorBuilder, Map<String, Object> params) throws IOException {
        String templateSource = templateSource(queryExtractorBuilder.query());

        if (templateSource.contains("{{") == false) {
            return queryExtractorBuilder;
        }

        Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, templateSource, Collections.emptyMap());
        String parsedTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(params).execute();
        // TODO: handle missing params.
        XContentParser parser = XContentType.JSON.xContent().createParser(parserConfiguration, parsedTemplate);

        return new QueryExtractorBuilder(
            queryExtractorBuilder.featureName(),
            QueryProvider.fromXContent(parser, false, INFERENCE_CONFIG_QUERY_BAD_FORMAT)
        );
    }

    private String templateSource(QueryProvider queryProvider) throws IOException {
        try (XContentBuilder configSourceBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            return BytesReference.bytes(queryProvider.toXContent(configSourceBuilder, EMPTY_PARAMS)).utf8ToString();
        }
    }
}
