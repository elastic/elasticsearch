/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.util.InferenceUtils;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.inference.util.InferenceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.core.inference.util.InferenceUtils.validateMapStringValues;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.ENABLED;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.MAX_NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.MIN_NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.rest.Paths.STREAM_SUFFIX;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

public final class ServiceUtils {

    public static AdaptiveAllocationsSettings removeAsAdaptiveAllocationsSettings(
        Map<String, Object> sourceMap,
        String key,
        ValidationException validationException
    ) {
        Map<String, Object> settingsMap = ServiceUtils.removeFromMap(sourceMap, key);
        if (settingsMap == null) {
            return null;
        }
        AdaptiveAllocationsSettings settings = new AdaptiveAllocationsSettings(
            InferenceUtils.removeAsType(settingsMap, ENABLED.getPreferredName(), Boolean.class, validationException),
            InferenceUtils.removeAsType(settingsMap, MIN_NUMBER_OF_ALLOCATIONS.getPreferredName(), Integer.class, validationException),
            InferenceUtils.removeAsType(settingsMap, MAX_NUMBER_OF_ALLOCATIONS.getPreferredName(), Integer.class, validationException)
        );
        for (String settingName : settingsMap.keySet()) {
            validationException.addValidationError(invalidSettingError(settingName, key));
        }
        ActionRequestValidationException exception = settings.validate();
        if (exception != null) {
            validationException.addValidationErrors(exception.validationErrors());
        }
        return settings;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> removeFromMap(Map<String, Object> sourceMap, String fieldName) {
        return (Map<String, Object>) sourceMap.remove(fieldName);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> removeFromMapOrThrowIfNull(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> removeFromMapOrDefaultEmpty(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> value = (Map<String, Object>) sourceMap.remove(fieldName);
        if (value == null) {
            return new HashMap<>();
        }
        return value;
    }

    public static String removeStringOrThrowIfNull(Map<String, Object> sourceMap, String key) {
        String value = InferenceUtils.removeAsType(sourceMap, key, String.class);
        if (value == null) {
            throw new ElasticsearchStatusException("Missing required field [{}]", RestStatus.BAD_REQUEST, key);
        }
        return value;
    }

    public static void throwIfNotEmptyMap(Map<String, Object> settingsMap, String serviceName) {
        if (settingsMap != null && settingsMap.isEmpty() == false) {
            throw ServiceUtils.unknownSettingsError(settingsMap, serviceName);
        }
    }

    public static void throwIfNotEmptyMap(Map<String, Object> settingsMap, String field, String scope) {
        if (settingsMap != null && settingsMap.isEmpty() == false) {
            throw ServiceUtils.unknownSettingsError(settingsMap, field, scope);
        }
    }

    public static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config, String serviceName) {
        // TODO map as JSON
        return new ElasticsearchStatusException(
            "Configuration contains settings [{}] unknown to the [{}] service",
            RestStatus.BAD_REQUEST,
            config,
            serviceName
        );
    }

    public static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config, String field, String scope) {
        return new ElasticsearchStatusException(
            "Configuration contains unknown settings [{}] while parsing field [{}] for settings [{}]",
            RestStatus.BAD_REQUEST,
            config,
            field,
            scope
        );
    }

    public static ElasticsearchStatusException invalidModelTypeForUpdateModelWithEmbeddingDetails(Class<? extends Model> invalidModelType) {
        throw new ElasticsearchStatusException(
            Strings.format("Can't update embedding details for model with unexpected type %s", invalidModelType),
            RestStatus.BAD_REQUEST
        );
    }

    public static ElasticsearchStatusException invalidModelTypeForUpdateModelWithChatCompletionDetails(
        Class<? extends Model> invalidModelType
    ) {
        throw new ElasticsearchStatusException(
            Strings.format("Can't update chat completion details for model with unexpected type %s", invalidModelType),
            RestStatus.BAD_REQUEST
        );
    }

    public static String invalidUrlErrorMsg(String url, String settingName, String settingScope, String error) {
        return Strings.format("[%s] Invalid url [%s] received for field [%s]. Error: %s", settingScope, url, settingName, error);
    }

    public static String invalidSettingError(String settingName, String scope) {
        return Strings.format("[%s] does not allow the setting [%s]", scope, settingName);
    }

    public static URI extractUri(Map<String, Object> map, String fieldName, ValidationException validationException) {
        String parsedUrl = InferenceUtils.extractRequiredString(map, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return convertToUri(parsedUrl, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);
    }

    public static URI convertToUri(@Nullable String url, String settingName, String settingScope, ValidationException validationException) {
        try {
            return createOptionalUri(url);
        } catch (IllegalArgumentException cause) {
            validationException.addValidationError(ServiceUtils.invalidUrlErrorMsg(url, settingName, settingScope, cause.getMessage()));
            return null;
        }
    }

    public static URI createUri(String url) throws IllegalArgumentException {
        Objects.requireNonNull(url);

        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(format("unable to parse url [%s]. Reason: %s", url, e.getReason()), e);
        }
    }

    public static URI createOptionalUri(String url) {
        if (url == null) {
            return null;
        }

        return createUri(url);
    }

    public static SecureString extractRequiredSecureString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        String requiredField = InferenceUtils.extractRequiredString(map, settingName, scope, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            return null;
        }

        return new SecureString(Objects.requireNonNull(requiredField).toCharArray());
    }

    public static SecureString extractOptionalSecureString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        String optionalField = InferenceUtils.extractOptionalString(map, settingName, scope, validationException);

        if (validationException.validationErrors().isEmpty() == false || optionalField == null) {
            return null;
        }

        return new SecureString(optionalField.toCharArray());
    }

    public static SimilarityMeasure extractSimilarity(Map<String, Object> map, String scope, ValidationException validationException) {
        return extractOptionalEnum(
            map,
            SIMILARITY,
            scope,
            SimilarityMeasure::fromString,
            EnumSet.allOf(SimilarityMeasure.class),
            validationException
        );
    }

    public static Map<String, SecureString> convertMapStringsToSecureString(
        Map<String, ?> map,
        String settingName,
        ValidationException validationException
    ) {
        if (map == null) {
            return Map.of();
        }

        var validatedMap = validateMapStringValues(map, settingName, validationException, true);

        return validatedMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new SecureString(e.getValue().toCharArray())));
    }

    /**
     * task_type can be specified as either a URL parameter or in the
     * request body. Resolve which to use or throw if the settings are
     * inconsistent
     * @param urlTaskType Taken from the URL parameter. ANY means not specified.
     * @param bodyTaskType Taken from the request body. Maybe null
     * @return The resolved task type
     */
    public static TaskType resolveTaskType(TaskType urlTaskType, String bodyTaskType) {
        if (bodyTaskType == null) {
            if (urlTaskType == TaskType.ANY) {
                throw new ElasticsearchStatusException("model is missing required setting [task_type]", RestStatus.BAD_REQUEST);
            } else {
                return urlTaskType;
            }
        }

        TaskType parsedBodyTask = TaskType.fromStringOrStatusException(bodyTaskType);
        if (parsedBodyTask == TaskType.ANY) {
            throw new ElasticsearchStatusException("task_type [any] is not valid type for inference", RestStatus.BAD_REQUEST);
        }

        if (parsedBodyTask.isAnyOrSame(urlTaskType) == false) {
            throw new ElasticsearchStatusException(
                "Cannot resolve conflicting task_type parameter in the request URL [{}] and the request body [{}]",
                RestStatus.BAD_REQUEST,
                urlTaskType.toString(),
                bodyTaskType
            );
        }

        return parsedBodyTask;
    }

    public static String parsePersistedConfigErrorMsg(String inferenceEntityId, String serviceName) {
        return format(
            "Failed to parse stored model [%s] for [%s] service, please delete and add the service again",
            inferenceEntityId,
            serviceName
        );
    }

    public static ElasticsearchStatusException createInvalidModelException(Model model) {
        return new ElasticsearchStatusException(
            format(
                "The internal model was invalid, please delete the service [%s] with id [%s] and add it again.",
                model.getConfigurations().getService(),
                model.getConfigurations().getInferenceEntityId()
            ),
            RestStatus.INTERNAL_SERVER_ERROR
        );
    }

    public static SecureString apiKey(@Nullable ApiKeySecrets secrets) {
        // To avoid a possible null pointer throughout the code we'll create a noop api key of an empty array
        return secrets == null ? new SecureString(new char[0]) : secrets.apiKey();
    }

    public static <T> T nonNullOrDefault(@Nullable T requestValue, @Nullable T originalSettingsValue) {
        return requestValue == null ? originalSettingsValue : requestValue;
    }

    public static void throwUnsupportedUnifiedCompletionOperation(String serviceName) {
        throw new UnsupportedOperationException(Strings.format("The %s service does not support unified completion", serviceName));
    }

    public static String unsupportedTaskTypeForInference(Model model, EnumSet<TaskType> supportedTaskTypes) {
        return Strings.format(
            "Inference entity [%s] does not support task type [%s] for inference, the task type must be one of %s.",
            model.getInferenceEntityId(),
            model.getTaskType(),
            supportedTaskTypes
        );
    }

    public static String useChatCompletionUrlMessage(Model model) {
        return org.elasticsearch.common.Strings.format(
            "The task type for the inference entity is %s, please use the _inference/%s/%s/%s URL.",
            model.getTaskType(),
            model.getTaskType(),
            model.getInferenceEntityId(),
            STREAM_SUFFIX
        );
    }

    public static final EnumSet<InputType> VALID_INTERNAL_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    public static void validateInputTypeIsUnspecifiedOrInternal(InputType inputType, ValidationException validationException) {
        if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_INTERNAL_INPUT_TYPE_VALUES.contains(inputType) == false) {
            validationException.addValidationError(
                Strings.format("Invalid input_type [%s]. The input_type option is not supported by this service", inputType)
            );
        }
    }

    public static void validateInputTypeIsUnspecifiedOrInternal(
        InputType inputType,
        ValidationException validationException,
        String customErrorMessage
    ) {
        if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_INTERNAL_INPUT_TYPE_VALUES.contains(inputType) == false) {
            validationException.addValidationError(customErrorMessage);
        }
    }

    public static void validateInputTypeAgainstAllowlist(
        InputType inputType,
        EnumSet<InputType> allowedInputTypes,
        String name,
        ValidationException validationException
    ) {
        if (inputType != null && inputType != InputType.UNSPECIFIED && allowedInputTypes.contains(inputType) == false) {
            validationException.addValidationError(
                org.elasticsearch.common.Strings.format("Input type [%s] is not supported for [%s]", inputType, name)
            );
        }
    }

    public static void checkByteBounds(short value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
        }
    }

    /**
     * Resolves the inference timeout based on input type and cluster settings.
     *
     * @param timeout The provided timeout value, may be null
     * @param inputType The input type for the inference request
     * @param clusterService The cluster service to get timeout settings from
     * @return The resolved timeout value
     */
    public static TimeValue resolveInferenceTimeout(@Nullable TimeValue timeout, InputType inputType, ClusterService clusterService) {
        return null;
    }

    private ServiceUtils() {}
}
