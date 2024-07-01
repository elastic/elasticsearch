/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import java.util.Collection;
import java.util.List;

/**
 * Utilities for validating security settings.
 */
public final class SecuritySettingsUtil {

    /**
     * Validates that a given setting's value is not empty nor null.
     *
     * @param settingKey    The full setting key which is validated. Used for building a proper error messages.
     * @param settingValue  The value to validate that it's not null nor empty.
     */
    public static void verifyNonNullNotEmpty(final String settingKey, final String settingValue) {
        verifyNonNullNotEmpty(settingKey, settingValue, null);
    }

    /**
     * Validates that a given setting's value is not empty nor null and that it is one of the allowed values.
     *
     * @param settingKey    The full setting key which is validated. Used for building a proper error messages.
     * @param settingValue  The value to validate that it's not null nor empty and that is one of the allowed values.
     * @param allowedValues Optional allowed values, against which to validate the given setting value.
     *                      If provided, it will be checked that the setting value is one of these allowed values.
     */
    public static void verifyNonNullNotEmpty(final String settingKey, final String settingValue, final Collection<String> allowedValues) {
        assert settingValue != null : "Invalid null value for [" + settingKey + "].";
        if (settingValue.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty value for [" + settingKey + "].");
        }
        if (allowedValues != null) {
            if (allowedValues.contains(settingValue) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + settingValue + "] for [" + settingKey + "]. Allowed values are " + allowedValues + "."
                );
            }
        }
    }

    /**
     * Validates that a given setting's values are not empty nor null.
     *
     * @param settingKey     The full setting key which is validated. Used for building a proper error messages.
     * @param settingValues  The values to validate that are not null nor empty.
     */
    public static void verifyNonNullNotEmpty(final String settingKey, final List<String> settingValues) {
        verifyNonNullNotEmpty(settingKey, settingValues, null);
    }

    /**
     * Validates that a given setting's values are not empty nor null and that are one of the allowed values.
     *
     * @param settingKey     The full setting key which is validated. Used for building a proper error messages.
     * @param settingValues  The values to validate that are not null nor empty and that are one of the allowed values.
     * @param allowedValues  The allowed values against which to validate the given setting values.
     *                       If provided, this method will check that the setting values are one of these allowed values.
     */
    public static void verifyNonNullNotEmpty(
        final String settingKey,
        final List<String> settingValues,
        final Collection<String> allowedValues
    ) {
        assert settingValues != null : "Invalid null list of values for [" + settingKey + "].";
        if (settingValues.isEmpty()) {
            if (allowedValues == null) {
                throw new IllegalArgumentException("Invalid empty list for [" + settingKey + "].");
            } else {
                throw new IllegalArgumentException(
                    "Invalid empty list for [" + settingKey + "]. Allowed values are " + allowedValues + "."
                );
            }
        }
        for (final String settingValue : settingValues) {
            verifyNonNullNotEmpty(settingKey, settingValue, allowedValues);
        }
    }

    private SecuritySettingsUtil() {
        throw new IllegalAccessError("not allowed!");
    }

}
