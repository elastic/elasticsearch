/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.core.Nullable;

import java.util.Map;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.mustBeAPositiveIntegerErrorMessage;
import static org.elasticsearch.xpack.core.inference.InferenceUtils.mustBeLessThanOrEqualNumberErrorMessage;
import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.invalidTypeErrorMsg;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

public final class NumberParser {

    /**
     * Extract an optional long from the map. JSON may produce Integer or Long, so we accept any Number.
     */
    public static Long extractLong(Map<String, Object> map, String key, String root) {
        var number = ObjectParserUtils.removeAsType(map, key, root, Number.class);

        if (number == null) {
            return null;
        }

        if (number instanceof Long == false && number instanceof Integer == false) {
            throw new IllegalArgumentException(invalidTypeErrorMsg(key, root, number, Long.class.getSimpleName()));
        }

        return number.longValue();
    }

    /**
     * Extract an optional integer from the map. JSON may produce Integer or Long, so a Long is accepted as long as it fits in an int.
     */
    public static Integer extractInteger(Map<String, Object> map, String key, String root) {
        var number = ObjectParserUtils.removeAsType(map, key, root, Number.class);

        if (number == null) {
            return null;
        }

        if (number instanceof Integer integer) {
            return integer;
        }

        if (number instanceof Long longValue && longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
            return longValue.intValue();
        }

        throw new IllegalArgumentException(invalidTypeErrorMsg(key, root, number, Integer.class.getSimpleName()));
    }

    /**
     * Validates that an optional integer service setting, when present, is strictly positive, throwing an
     * {@link IllegalArgumentException} otherwise.
     */
    public static void validatePositiveInteger(@Nullable Integer value, String settingName) {
        if (value != null && value <= 0) {
            throw new IllegalArgumentException(mustBeAPositiveIntegerErrorMessage(settingName, SERVICE_SETTINGS.toString(), value));
        }
    }

    /**
     * Validates that an optional integer service setting, when present, is strictly positive and less than or equal to the max value,
     * throwing an {@link IllegalArgumentException} otherwise.
     */
    public static void validatePositiveIntegerLessThanOrEqualToMax(@Nullable Integer value, String settingName, int maxValue) {
        validatePositiveInteger(value, settingName);

        if (value != null && value > maxValue) {
            throw new IllegalArgumentException(
                mustBeLessThanOrEqualNumberErrorMessage(settingName, SERVICE_SETTINGS.toString(), value, maxValue)
            );
        }
    }

    private NumberParser() {}
}
