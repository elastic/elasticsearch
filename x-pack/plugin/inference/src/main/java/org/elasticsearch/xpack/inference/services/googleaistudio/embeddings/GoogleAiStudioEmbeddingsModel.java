/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.VALID_INTERNAL_INPUT_TYPE_VALUES;

public class GoogleAiStudioEmbeddingsModel extends GoogleAiStudioModel {
    static final String MODEL_ID_WITH_TASK_TYPE = "embedding-001";
    static final EnumSet<InputType> VALID_REQUEST_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    public static GoogleAiStudioEmbeddingsModel of(GoogleAiStudioEmbeddingsModel model, InputType inputType) {
        var modelId = model.getServiceSettings().modelId();
        // InputType is only allowed when model=embedding-001 https://ai.google.dev/api/embeddings?authuser=5#EmbedContentRequest
        ValidationException validationException = new ValidationException();
        if (Objects.equals(model.getServiceSettings().modelId(), MODEL_ID_WITH_TASK_TYPE) == false) {
            // this model does not accept input type parameter
            if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_INTERNAL_INPUT_TYPE_VALUES.contains(inputType) == false) {
                // throw validation exception if ingest type is specified
                validationException.addValidationError(
                    Strings.format("Invalid value [%s] received. [%s] is not allowed for model [%s]", inputType, "input_type", modelId)
                );
            } else {
                return model;
            }
        } else {
            if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_REQUEST_VALUES.contains(inputType) == false) {
                validationException.addValidationError(
                    Strings.format("Invalid value [%s] received. [%s] is not allowed", inputType, "input_type")
                );
            }
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return new GoogleAiStudioEmbeddingsModel(model, model.getServiceSettings(), inputType == InputType.UNSPECIFIED ? null : inputType);
    }

    private URI uri;
    private InputType inputType;

    public GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleAiStudioEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public GoogleAiStudioEmbeddingsModel(GoogleAiStudioEmbeddingsModel model, GoogleAiStudioEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public GoogleAiStudioEmbeddingsModel(
        GoogleAiStudioEmbeddingsModel model,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        InputType inputType
    ) {
        super(model, serviceSettings);
        this.inputType = inputType;
        try {
            this.uri = buildUri(serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secrets,
        @Nullable InputType inputType
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        this.inputType = inputType;
        try {
            this.uri = buildUri(serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingsettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingsettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GoogleAiStudioEmbeddingsServiceSettings getServiceSettings() {
        return (GoogleAiStudioEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return uri;
    }

    public InputType inputType() {
        return inputType;
    }

    public static URI buildUri(String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleAiStudioUtils.HOST_SUFFIX)
            .setPathSegments(
                GoogleAiStudioUtils.V1,
                GoogleAiStudioUtils.MODELS,
                format("%s:%s", model, GoogleAiStudioUtils.BATCH_EMBED_CONTENTS_ACTION)
            )
            .build();
    }
}
