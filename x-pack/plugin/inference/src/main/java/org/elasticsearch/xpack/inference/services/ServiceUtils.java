/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbedding;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.ENABLED;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.MAX_NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.MIN_NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

public final class ServiceUtils {
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
            throw new ElasticsearchStatusException(invalidTypeErrorMsg(key, o, type.getSimpleName()), RestStatus.BAD_REQUEST);
        }
    }

    /**
     * Remove the object from the map and cast to the expected type.
     * If the object cannot be cast to type and error is added to the
     * {@code validationException} parameter
     *
     * @param sourceMap Map containing fields
     * @param key The key of the object to remove
     * @param type The expected type of the removed object
     * @param validationException If the value is not of type {@code type}
     * @return {@code null} if not present else the object cast to type T
     * @param <T> The expected type
     */
    @SuppressWarnings("unchecked")
    public static <T> T removeAsType(Map<String, Object> sourceMap, String key, Class<T> type, ValidationException validationException) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            return null;
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            validationException.addValidationError(invalidTypeErrorMsg(key, o, type.getSimpleName()));
            return null;
        }
    }

    /**
     * Remove the object from the map and cast to first assignable type in the expected types list.
     * If the object cannot be cast to one of the types an error is added to the
     * {@code validationException} parameter
     *
     * @param sourceMap Map containing fields
     * @param key The key of the object to remove
     * @param types The expected types of the removed object
     * @param validationException If the value is not of type {@code type}
     * @return {@code null} if not present else the object cast to the first assignable type in the types list
     */
    public static Object removeAsOneOfTypes(
        Map<String, Object> sourceMap,
        String key,
        List<Class<?>> types,
        ValidationException validationException
    ) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            return null;
        }

        for (Class<?> type : types) {
            if (type.isAssignableFrom(o.getClass())) {
                return type.cast(o);
            }
        }

        validationException.addValidationError(
            invalidTypesErrorMsg(key, o, types.stream().map(Class::getSimpleName).collect(Collectors.toList()))
        );
        return null;
    }

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
            ServiceUtils.removeAsType(settingsMap, ENABLED.getPreferredName(), Boolean.class, validationException),
            ServiceUtils.removeAsType(settingsMap, MIN_NUMBER_OF_ALLOCATIONS.getPreferredName(), Integer.class, validationException),
            ServiceUtils.removeAsType(settingsMap, MAX_NUMBER_OF_ALLOCATIONS.getPreferredName(), Integer.class, validationException)
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

    public static ElasticsearchStatusException invalidModelTypeForUpdateModelWithEmbeddingDetails(Class<? extends Model> invalidModelType) {
        throw new ElasticsearchStatusException(
            Strings.format("Can't update embedding details for model with unexpected type %s", invalidModelType),
            RestStatus.BAD_REQUEST
        );
    }

    public static String missingSettingErrorMsg(String settingName, String scope) {
        return Strings.format("[%s] does not contain the required setting [%s]", scope, settingName);
    }

    public static String missingOneOfSettingsErrorMsg(List<String> settingNames, String scope) {
        return Strings.format("[%s] does not contain one of the required settings [%s]", scope, String.join(", ", settingNames));
    }

    public static String invalidTypeErrorMsg(String settingName, Object foundObject, String expectedType) {
        return Strings.format(
            "field [%s] is not of the expected type. The value [%s] cannot be converted to a [%s]",
            settingName,
            foundObject,
            expectedType
        );
    }

    public static String invalidTypesErrorMsg(String settingName, Object foundObject, List<String> expectedTypes) {
        return Strings.format(
            // omitting [ ] for the last string as this will be added, if you convert the list to a string anyway
            "field [%s] is not of one of the expected types. The value [%s] cannot be converted to one of %s",
            settingName,
            foundObject,
            expectedTypes
        );
    }

    public static String invalidUrlErrorMsg(String url, String settingName, String settingScope, String error) {
        return Strings.format("[%s] Invalid url [%s] received for field [%s]. Error: %s", settingScope, url, settingName, error);
    }

    public static String mustBeNonEmptyString(String settingName, String scope) {
        return Strings.format("[%s] Invalid value empty string. [%s] must be a non-empty string", scope, settingName);
    }

    public static String invalidTimeValueMsg(String timeValueStr, String settingName, String scope, String exceptionMsg) {
        return Strings.format(
            "[%s] Invalid time value [%s]. [%s] must be a valid time value string: %s",
            scope,
            timeValueStr,
            settingName,
            exceptionMsg
        );
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

    public static String invalidSettingError(String settingName, String scope) {
        return Strings.format("[%s] does not allow the setting [%s]", scope, settingName);
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
        String requiredField = extractRequiredString(map, settingName, scope, validationException);

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
        String optionalField = extractOptionalString(map, settingName, scope, validationException);

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

    public static String extractRequiredString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        String requiredField = ServiceUtils.removeAsType(map, settingName, String.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            // new validation error occurred
            return null;
        }

        if (requiredField == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(settingName, scope));
        } else if (requiredField.isEmpty()) {
            validationException.addValidationError(ServiceUtils.mustBeNonEmptyString(settingName, scope));
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
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
        int initialValidationErrorCount = validationException.validationErrors().size();
        String optionalField = ServiceUtils.removeAsType(map, settingName, String.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            // new validation error occurred
            return null;
        }

        if (optionalField != null && optionalField.isEmpty()) {
            validationException.addValidationError(ServiceUtils.mustBeNonEmptyString(settingName, scope));
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return optionalField;
    }

    public static Integer extractRequiredPositiveInteger(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        Integer field = ServiceUtils.removeAsType(map, settingName, Integer.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (field == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(settingName, scope));
        } else if (field <= 0) {
            validationException.addValidationError(ServiceUtils.mustBeAPositiveIntegerErrorMessage(settingName, scope, field));
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return field;
    }

    public static Integer extractRequiredPositiveIntegerLessThanOrEqualToMax(
        Map<String, Object> map,
        String settingName,
        int maxValue,
        String scope,
        ValidationException validationException
    ) {
        Integer field = extractRequiredPositiveInteger(map, settingName, scope, validationException);

        if (field != null && field > maxValue) {
            validationException.addValidationError(
                ServiceUtils.mustBeLessThanOrEqualNumberErrorMessage(settingName, scope, field, maxValue)
            );
        }

        return field;
    }

    public static Integer extractRequiredPositiveIntegerBetween(
        Map<String, Object> map,
        String settingName,
        int minValue,
        int maxValue,
        String scope,
        ValidationException validationException
    ) {
        Integer field = extractRequiredPositiveInteger(map, settingName, scope, validationException);

        if (field != null && field < minValue) {
            validationException.addValidationError(
                ServiceUtils.mustBeGreaterThanOrEqualNumberErrorMessage(settingName, scope, field, minValue)
            );
            return null;
        }
        if (field != null && field > maxValue) {
            validationException.addValidationError(
                ServiceUtils.mustBeLessThanOrEqualNumberErrorMessage(settingName, scope, field, maxValue)
            );
            return null;
        }

        return field;
    }

    public static Integer extractOptionalPositiveInteger(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        Integer optionalField = ServiceUtils.removeAsType(map, settingName, Integer.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (optionalField != null && optionalField <= 0) {
            validationException.addValidationError(ServiceUtils.mustBeAPositiveIntegerErrorMessage(settingName, scope, optionalField));
            return null;
        }

        return optionalField;
    }

    public static Float extractOptionalFloat(Map<String, Object> map, String settingName) {
        return ServiceUtils.removeAsType(map, settingName, Float.class);
    }

    public static Double extractOptionalDoubleInRange(
        Map<String, Object> map,
        String settingName,
        @Nullable Double minValue,
        @Nullable Double maxValue,
        String scope,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        var doubleReturn = ServiceUtils.removeAsType(map, settingName, Double.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (doubleReturn != null && minValue != null && doubleReturn < minValue) {
            validationException.addValidationError(
                ServiceUtils.mustBeGreaterThanOrEqualNumberErrorMessage(settingName, scope, doubleReturn, minValue)
            );
        }

        if (doubleReturn != null && maxValue != null && doubleReturn > maxValue) {
            validationException.addValidationError(
                ServiceUtils.mustBeLessThanOrEqualNumberErrorMessage(settingName, scope, doubleReturn, maxValue)
            );
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return doubleReturn;
    }

    public static <E extends Enum<E>> E extractRequiredEnum(
        Map<String, Object> map,
        String settingName,
        String scope,
        EnumConstructor<E> constructor,
        EnumSet<E> validValues,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        var enumReturn = extractOptionalEnum(map, settingName, scope, constructor, validValues, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (enumReturn == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(settingName, scope));
        }

        return enumReturn;
    }

    public static Long extractOptionalPositiveLong(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        // We don't want callers to handle the implementation detail that a long is expected (also treat integers like a long)
        List<Class<?>> types = List.of(Integer.class, Long.class);
        int initialValidationErrorCount = validationException.validationErrors().size();
        var optionalField = ServiceUtils.removeAsOneOfTypes(map, settingName, types, validationException);

        if (optionalField != null) {
            try {
                // Use String.valueOf first as there's no Long.valueOf(Object o)
                Long longValue = Long.valueOf(String.valueOf(optionalField));

                if (longValue <= 0L) {
                    validationException.addValidationError(ServiceUtils.mustBeAPositiveLongErrorMessage(settingName, scope, longValue));
                }

                if (validationException.validationErrors().size() > initialValidationErrorCount) {
                    return null;
                }

                return longValue;
            } catch (NumberFormatException e) {
                validationException.addValidationError(format("unable to parse long [%s]", e));
            }
        }

        return null;
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

        try {
            var createdEnum = constructor.apply(enumString);
            validateEnumValue(createdEnum, validValues);

            return createdEnum;
        } catch (IllegalArgumentException e) {
            var validValuesAsStrings = validValues.stream().map(value -> value.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);
            validationException.addValidationError(invalidValue(settingName, scope, enumString, validValuesAsStrings));
        }

        return null;
    }

    public static Boolean extractOptionalBoolean(Map<String, Object> map, String settingName, ValidationException validationException) {
        return ServiceUtils.removeAsType(map, settingName, Boolean.class, validationException);
    }

    public static TimeValue extractOptionalTimeValue(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        var timeValueString = extractOptionalString(map, settingName, scope, validationException);
        if (timeValueString == null) {
            return null;
        }

        try {
            return TimeValue.parseTimeValue(timeValueString, settingName);
        } catch (Exception e) {
            validationException.addValidationError(invalidTimeValueMsg(timeValueString, settingName, scope, e.getMessage()));
        }

        return null;
    }

    private static <E extends Enum<E>> void validateEnumValue(E enumValue, EnumSet<E> validValues) {
        if (validValues.contains(enumValue) == false) {
            throw new IllegalArgumentException(Strings.format("Enum value [%s] is not one of the acceptable values", enumValue.toString()));
        }
    }

    public static String mustBeAPositiveIntegerErrorMessage(String settingName, String scope, int value) {
        return format("[%s] Invalid value [%s]. [%s] must be a positive integer", scope, value, settingName);
    }

    public static String mustBeLessThanOrEqualNumberErrorMessage(String settingName, String scope, double value, double maxValue) {
        return format("[%s] Invalid value [%s]. [%s] must be a less than or equal to [%s]", scope, value, settingName, maxValue);
    }

    public static String mustBeGreaterThanOrEqualNumberErrorMessage(String settingName, String scope, double value, double minValue) {
        return format("[%s] Invalid value [%s]. [%s] must be a greater than or equal to [%s]", scope, value, settingName, minValue);
    }

    public static String mustBeAFloatingPointNumberErrorMessage(String settingName, String scope) {
        return format("[%s] Invalid value. [%s] must be a floating point number", scope, settingName);
    }

    public static String mustBeAPositiveLongErrorMessage(String settingName, String scope, Long value) {
        return format("[%s] Invalid value [%s]. [%s] must be a positive long", scope, value, settingName);
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

        service.infer(
            model,
            null,
            List.of(TEST_EMBEDDING_INPUT),
            false,
            Map.of(),
            InputType.INGEST,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener.delegateFailureAndWrap((delegate, r) -> {
                if (r instanceof TextEmbedding embeddingResults) {
                    try {
                        delegate.onResponse(embeddingResults.getFirstEmbeddingSize());
                    } catch (Exception e) {
                        delegate.onFailure(
                            new ElasticsearchStatusException("Could not determine embedding size", RestStatus.BAD_REQUEST, e)
                        );
                    }
                } else {
                    delegate.onFailure(
                        new ElasticsearchStatusException(
                            "Could not determine embedding size. "
                                + "Expected a result of type ["
                                + InferenceTextEmbeddingFloatResults.NAME
                                + "] got ["
                                + r.getWriteableName()
                                + "]",
                            RestStatus.BAD_REQUEST
                        )
                    );
                }
            })
        );
    }

    private static final String TEST_EMBEDDING_INPUT = "how big";

    public static SecureString apiKey(@Nullable ApiKeySecrets secrets) {
        // To avoid a possible null pointer throughout the code we'll create a noop api key of an empty array
        return secrets == null ? new SecureString(new char[0]) : secrets.apiKey();
    }

    public static <T> T nonNullOrDefault(@Nullable T requestValue, @Nullable T originalSettingsValue) {
        return requestValue == null ? originalSettingsValue : requestValue;
    }

    private ServiceUtils() {}
}
