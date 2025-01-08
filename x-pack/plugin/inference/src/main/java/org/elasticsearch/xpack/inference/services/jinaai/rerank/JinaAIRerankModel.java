/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.jinaai.JinaAIActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings.RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings.TOP_N_DOCS_ONLY;

public class JinaAIRerankModel extends JinaAIModel {
    public static JinaAIRerankModel of(JinaAIRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = JinaAIRerankTaskSettings.fromMap(taskSettings);
        return new JinaAIRerankModel(model, JinaAIRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public JinaAIRerankModel(
        String inferenceId,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            service,
            JinaAIRerankServiceSettings.fromMap(serviceSettings, context),
            JinaAIRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    JinaAIRerankModel(
        String modelId,
        String service,
        JinaAIRerankServiceSettings serviceSettings,
        JinaAIRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings()
        );
    }

    private JinaAIRerankModel(JinaAIRerankModel model, JinaAIRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public JinaAIRerankModel(JinaAIRerankModel model, JinaAIRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public JinaAIRerankServiceSettings getServiceSettings() {
        return (JinaAIRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public JinaAIRerankTaskSettings getTaskSettings() {
        return (JinaAIRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor _
     * @param taskSettings _
     * @param inputType ignored for rerank task
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(JinaAIActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public URI uri() {
        return getServiceSettings().getCommonSettings().uri();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    RETURN_DOCUMENTS,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TOGGLE)
                        .setLabel("Return Documents")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specify whether to return doc text within the results.")
                        .setType(SettingsConfigurationFieldType.BOOLEAN)
                        .setValue(false)
                        .build()
                );
                configurationMap.put(
                    TOP_N_DOCS_ONLY,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Top N")
                        .setOrder(2)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("The number of most relevant documents to return, defaults to the number of the documents.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
