/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.TextEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

public class ServiceUtils {
    /**
     * Remove the object from the map and cast to the expected type.
     * If the object cannot be cast to type an ElasticsearchStatusException
     * is thrown.
     *
     * @param sourceMap Map containing fields
     * @param key The key of the object to remove
     * @param type The expected type of the removed object
     * @return {@code null} if not present else the object cast to type T
     * @param <T> The expected type
     */
    @SuppressWarnings("unchecked")
    public static <T> T removeAsType(Map<String, Object> sourceMap, String key, Class<T> type) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            return null;
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            throw new ElasticsearchStatusException(
                "field [{}] is not of the expected type." + " The value [{}] cannot be converted to a [{}]",
                RestStatus.BAD_REQUEST,
                key,
                o,
                type.getSimpleName()
            );
        }
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
        String value = removeAsType(sourceMap, key, String.class);
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

    public static ElasticsearchStatusException unknownSettingsError(Map<String, Object> config, String serviceName) {
        // TODO map as JSON
        return new ElasticsearchStatusException(
            "Model configuration contains settings [{}] unknown to the [{}] service",
            RestStatus.BAD_REQUEST,
            config,
            serviceName
        );
    }

    public static String missingSettingErrorMsg(String settingName, String scope) {
        return Strings.format("[%s] does not contain the required setting [%s]", scope, settingName);
    }

    public static String invalidUrlErrorMsg(String url, String settingName, String settingScope) {
        return Strings.format("[%s] Invalid url [%s] received for field [%s]", settingScope, url, settingName);
    }

    public static String mustBeNonEmptyString(String settingName, String scope) {
        return Strings.format("[%s] Invalid value empty string. [%s] must be a non-empty string", scope, settingName);
    }

    public static String invalidValue(String settingName, String scope, String invalidType, String[] requiredValues) {
        var copyOfRequiredValues = requiredValues.clone();
        Arrays.sort(copyOfRequiredValues);

        return Strings.format(
            "[%s] Invalid value [%s] received. [%s] must be one of [%s]",
            scope,
            invalidType,
            settingName,
            String.join(", ", copyOfRequiredValues)
        );
    }

    // TODO improve URI validation logic
    public static URI convertToUri(@Nullable String url, String settingName, String settingScope, ValidationException validationException) {
        try {
            if (url == null) {
                return null;
            }

            return createUri(url);
        } catch (IllegalArgumentException ignored) {
            validationException.addValidationError(ServiceUtils.invalidUrlErrorMsg(url, settingName, settingScope));
            return null;
        }
    }

    public static URI createUri(String url) throws IllegalArgumentException {
        Objects.requireNonNull(url);

        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(format("unable to parse url [%s]", url), e);
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
        String requiredField = extractRequiredString(map, settingName, scope, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            return null;
        }

        return new SecureString(Objects.requireNonNull(requiredField).toCharArray());
    }

    public static SimilarityMeasure extractSimilarity(Map<String, Object> map, String scope, ValidationException validationException) {
        String similarity = extractOptionalString(map, SIMILARITY, scope, validationException);

        if (similarity != null) {
            try {
                return SimilarityMeasure.fromString(similarity);
            } catch (IllegalArgumentException iae) {
                validationException.addValidationError("[" + scope + "] Unknown similarity measure [" + similarity + "]");
            }
        }

        return null;
    }

    public static String extractRequiredString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        String requiredField = ServiceUtils.removeAsType(map, settingName, String.class);

        if (requiredField == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(settingName, scope));
        } else if (requiredField.isEmpty()) {
            validationException.addValidationError(ServiceUtils.mustBeNonEmptyString(settingName, scope));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            return null;
        }

        return requiredField;
    }

    public static String extractOptionalString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        String optionalField = ServiceUtils.removeAsType(map, settingName, String.class);

        if (optionalField != null && optionalField.isEmpty()) {
            validationException.addValidationError(ServiceUtils.mustBeNonEmptyString(settingName, scope));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            return null;
        }

        return optionalField;
    }

    public static <E extends Enum<E>> E extractOptionalEnum(
        Map<String, Object> map,
        String settingName,
        String scope,
        EnumConstructor<E> constructor,
        EnumSet<E> validValues,
        ValidationException validationException
    ) {
        var enumString = extractOptionalString(map, settingName, scope, validationException);
        if (enumString == null) {
            return null;
        }

        var validValuesAsStrings = validValues.stream().map(value -> value.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);

        try {
            var createdEnum = constructor.apply(enumString);
            validateEnumValue(createdEnum, validValues);

            return createdEnum;
        } catch (IllegalArgumentException e) {
            validationException.addValidationError(invalidValue(settingName, scope, enumString, validValuesAsStrings));
        }

        return null;
    }

    private static <E extends Enum<E>> void validateEnumValue(E enumValue, EnumSet<E> validValues) {
        if (validValues.contains(enumValue) == false) {
            throw new IllegalArgumentException(Strings.format("Enum value [%s] is not one of the acceptable values", enumValue.toString()));
        }
    }

    public static String mustBeAPositiveNumberErrorMessage(String settingName, int value) {
        if (value <= 0) {
            return "Invalid value [" + value + "]. [" + settingName + "] must be a positive integer";
        } else {
            throw new IllegalArgumentException("Value [" + value + "] is not a positive integer");
        }
    }

    /**
     * Functional interface for creating an enum from a string.
     * @param <E>
     */
    @FunctionalInterface
    public interface EnumConstructor<E extends Enum<E>> {
        E apply(String name) throws IllegalArgumentException;
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

    /**
     * Evaluate the model and return the text embedding size
     * @param model Should be a text embedding model
     * @param service The inference service
     * @param listener Size listener
     */
    public static void getEmbeddingSize(Model model, InferenceService service, ActionListener<Integer> listener) {
        assert model.getTaskType() == TaskType.TEXT_EMBEDDING;

        service.infer(model, List.of(TEST_EMBEDDING_INPUT), Map.of(), InputType.INGEST, listener.delegateFailureAndWrap((delegate, r) -> {
            if (r instanceof TextEmbedding embeddingResults) {
                try {
                    delegate.onResponse(embeddingResults.getFirstEmbeddingSize());
                } catch (Exception e) {
                    delegate.onFailure(new ElasticsearchStatusException("Could not determine embedding size", RestStatus.BAD_REQUEST, e));
                }
            } else {
                delegate.onFailure(
                    new ElasticsearchStatusException(
                        "Could not determine embedding size. "
                            + "Expected a result of type ["
                            + TextEmbeddingResults.NAME
                            + "] got ["
                            + r.getWriteableName()
                            + "]",
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }));
    }

    private static final String TEST_EMBEDDING_INPUT = "how big";
}
