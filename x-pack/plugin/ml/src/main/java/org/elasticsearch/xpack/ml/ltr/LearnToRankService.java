/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
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
import java.util.Optional;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class LearnToRankService {
    private final ModelLoadingService modelLoadingService;
    private final ScriptService scriptService;
    private final XContentParserConfiguration parserConfiguration;

    public LearnToRankService(
        ModelLoadingService modelLoadingService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry
    ) {
        this(modelLoadingService, scriptService, XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry));
    }

    LearnToRankService(
        ModelLoadingService modelLoadingService,
        ScriptService scriptService,
        XContentParserConfiguration parserConfiguration
    ) {
        this.modelLoadingService = modelLoadingService;
        this.scriptService = scriptService;
        this.parserConfiguration = parserConfiguration;
    }

    public void loadLocalModel(String modelId, ActionListener<LocalModel> listener) {
        modelLoadingService.getModelForLearnToRank(modelId, listener);
    }

    public void loadLearnToRankConfig(
        Client client,
        String modelId,
        Map<String, Object> params,
        ActionListener<LearnToRankConfig> listener
    ) {
        GetTrainedModelsAction.Request request = new GetTrainedModelsAction.Request(modelId);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            GetTrainedModelsAction.INSTANCE,
            request,
            ActionListener.wrap(trainedModels -> {
                TrainedModelConfig config = trainedModels.getResources().results().get(0);
                if (config.getInferenceConfig() instanceof LearnToRankConfig retrievedInferenceConfig) {
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
                            Optional.ofNullable(config.getInferenceConfig()).map(InferenceConfig::getName).orElse("null"),
                            LearnToRankConfig.NAME.getPreferredName()
                        )
                    )
                );
                listener.onResponse(null);
            }, listener::onFailure)
        );
    }

    private LearnToRankConfig applyParams(LearnToRankConfig config, Map<String, Object> params) throws IOException {
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

            XContentParser parser = XContentType.JSON.xContent().createParser(parserConfiguration, parsedTemplate);

            return LearnToRankConfig.fromXContentStrict(parser);
        }
    }
}
