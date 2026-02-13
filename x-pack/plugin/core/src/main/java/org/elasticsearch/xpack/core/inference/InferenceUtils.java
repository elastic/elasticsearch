/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Strings;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class InferenceUtils {

    private InferenceUtils() {}

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
        if (sourceMap == null) {
            validationException.addValidationError(Strings.format("Encountered a null input map while parsing field [%s]", key));
            return null;
        }

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

    public static String extractOptionalString(
        Map<String, Object> map,
        String settingName,
        String scope,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        String optionalField = removeAsType(map, settingName, String.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            // new validation error occurred
            return null;
        }

        if (optionalField != null && optionalField.isEmpty()) {
            validationException.addValidationError(mustBeNonEmptyString(settingName, scope));
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
        Integer field = InferenceUtils.removeAsType(map, settingName, Integer.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (field == null) {
            validationException.addValidationError(InferenceUtils.missingSettingErrorMsg(settingName, scope));
        } else if (field <= 0) {
            validationException.addValidationError(InferenceUtils.mustBeAPositiveIntegerErrorMessage(settingName, scope, field));
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return field;
    }

    public static Integer extractRequiredPositiveIntegerGreaterThanOrEqualToMin(
        Map<String, Object> map,
        String settingName,
        int minValue,
        String scope,
        ValidationException validationException
    ) {
        Integer field = extractRequiredPositiveInteger(map, settingName, scope, validationException);

        if (field != null && field < minValue) {
            validationException.addValidationError(
                InferenceUtils.mustBeGreaterThanOrEqualNumberErrorMessage(settingName, scope, field, minValue)
            );
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
                InferenceUtils.mustBeLessThanOrEqualNumberErrorMessage(settingName, scope, field, maxValue)
            );
        }

        return field;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> extractOptionalList(
        Map<String, Object> map,
        String settingName,
        Class<T> type,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        var optionalField = InferenceUtils.removeAsType(map, settingName, List.class, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        if (optionalField != null) {
            for (Object o : optionalField) {
                if (o.getClass().equals(type) == false) {
                    validationException.addValidationError(InferenceUtils.invalidTypeErrorMsg(settingName, o, "String"));
                }
            }
        }

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return (List<T>) optionalField;
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

    public static <E extends Enum<E>> void validateEnumValue(E enumValue, EnumSet<E> validValues) {
        if (validValues.contains(enumValue) == false) {
            throw new IllegalArgumentException(
                Strings.format("Enum value [%s] is not one of the acceptable values: %s", enumValue.toString(), validValues.toString())
            );
        }
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

    public static String invalidTypeErrorMsg(String settingName, Object foundObject, String expectedType) {
        return Strings.format(
            "field [%s] is not of the expected type. The value [%s] cannot be converted to a [%s]",
            settingName,
            foundObject,
            expectedType
        );
    }

    public static String missingSettingErrorMsg(String settingName, String scope) {
        return Strings.format("[%s] does not contain the required setting [%s]", scope, settingName);
    }

    public static String mustBeGreaterThanOrEqualNumberErrorMessage(String settingName, String scope, double value, double minValue) {
        return format("[%s] Invalid value [%s]. [%s] must be greater than or equal to [%s]", scope, value, settingName, minValue);
    }

    public static String mustBeLessThanOrEqualNumberErrorMessage(String settingName, String scope, double value, double maxValue) {
        return format("[%s] Invalid value [%s]. [%s] must be less than or equal to [%s]", scope, value, settingName, maxValue);
    }

    public static String mustBeAPositiveIntegerErrorMessage(String settingName, String scope, int value) {
        return format("[%s] Invalid value [%s]. [%s] must be a positive integer", scope, value, settingName);
    }

    /**
     * Functional interface for creating an enum from a string.
     * @param <E>
     */
    @FunctionalInterface
    public interface EnumConstructor<E extends Enum<E>> {
        E apply(String name) throws IllegalArgumentException;
    }
}
