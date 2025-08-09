/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.TaskType.unsupportedTaskTypeErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

@SuppressWarnings("checkstyle:LineLength")
public class PredefinedCustomService extends CustomService {

    private static PredefinedCustomServiceSchema SCHEMA;

    public PredefinedCustomService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context,
        PredefinedCustomServiceSchema schema
    ) {
        super(factory, serviceComponents, context);
        SCHEMA = schema;
    }

    @Override
    public String name() {
        return SCHEMA.getName();
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            // Parse service settings map to ensure it contains the expected structure
            Map<String, Object> parsedServiceSettingsMap = parseServiceSettingsMap(serviceSettingsMap);

            PredefinedCustomModel model = createModel(
                inferenceEntityId,
                taskType,
                parsedServiceSettingsMap,
                taskSettingsMap,
                parsedServiceSettingsMap,
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, SCHEMA.getName());
            throwIfNotEmptyMap(parsedServiceSettingsMap, SCHEMA.getName());
            throwIfNotEmptyMap(taskSettingsMap, SCHEMA.getName());

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public CustomModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        // Parse service settings map to ensure it contains the expected structure
        Map<String, Object> parsedServiceSettingsMap = parseServiceSettingsMap(serviceSettingsMap);

        return createModel(
            inferenceEntityId,
            taskType,
            parsedServiceSettingsMap,
            taskSettingsMap,
            parsedServiceSettingsMap,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public CustomModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        // Parse service settings map to ensure it contains the expected structure
        Map<String, Object> parsedServiceSettingsMap = parseServiceSettingsMap(serviceSettingsMap);

        return createModel(
            inferenceEntityId,
            taskType,
            parsedServiceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof PredefinedCustomModel customModel && customModel.getTaskType() == TaskType.TEXT_EMBEDDING) {
            var newServiceSettings = new PredefinedServiceSettings(getCustomServiceSettings(customModel, embeddingSize));

            return new PredefinedCustomModel(customModel, newServiceSettings);
        } else {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "Can't update embedding details for model of type: [%s], task type: [%s]",
                    model.getClass().getSimpleName(),
                    model.getTaskType()
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }

    // TODO: parseRequestConfig should store a model in the index that matches the old pre defined service format (no schema information)

    private static PredefinedCustomModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> parsedServiceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        if (SCHEMA.getSupportedTaskTypes().contains(taskType) == false) {
            throw new ElasticsearchStatusException(unsupportedTaskTypeErrorMsg(taskType, NAME), RestStatus.BAD_REQUEST);
        }
        return new PredefinedCustomModel(
            inferenceEntityId,
            taskType,
            SCHEMA.getName(),
            parsedServiceSettings,
            taskSettings,
            secretSettings,
            context
        );
    }

    private Map<String, Object> parseServiceSettingsMap(Map<String, Object> unparsedServiceSettingsMap) {
        Map<String, Object> parsedServiceSettingsMap = new HashMap<>(parseSchema());

        Map<String, Object> secretParameters = new HashMap<>();
        for (String secretParameter : SCHEMA.getSecretParameters()) {
            if (unparsedServiceSettingsMap.containsKey(secretParameter)) {
                secretParameters.put(secretParameter, unparsedServiceSettingsMap.remove(secretParameter));
            }
        }
        parsedServiceSettingsMap.put("secret_parameters", secretParameters);

        Map<String, Object> parameters = new HashMap<>();
        for (String parameter : SCHEMA.getParameters()) {
            if (unparsedServiceSettingsMap.containsKey(parameter)) {
                parameters.put(parameter, unparsedServiceSettingsMap.remove(parameter));
            }
        }
        parsedServiceSettingsMap.put("parameters", parameters);
        return parsedServiceSettingsMap;
    }

    private Map<String, Object> parseSchema() {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, SCHEMA.getSchema())) {
            return parser.map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
