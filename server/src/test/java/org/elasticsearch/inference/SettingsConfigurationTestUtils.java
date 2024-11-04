/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.inference.configuration.SettingsConfigurationDependency;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.configuration.SettingsConfigurationSelectOption;
import org.elasticsearch.inference.configuration.SettingsConfigurationValidation;
import org.elasticsearch.inference.configuration.SettingsConfigurationValidationType;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class SettingsConfigurationTestUtils {

    public static SettingsConfiguration getRandomSettingsConfigurationField() {
        return new SettingsConfiguration.Builder().setCategory(randomAlphaOfLength(10))
            .setDefaultValue(randomAlphaOfLength(10))
            .setDependsOn(List.of(getRandomSettingsConfigurationDependency()))
            .setDisplay(getRandomSettingsConfigurationDisplayType())
            .setLabel(randomAlphaOfLength(10))
            .setOptions(List.of(getRandomSettingsConfigurationSelectOption(), getRandomSettingsConfigurationSelectOption()))
            .setOrder(randomInt())
            .setPlaceholder(randomAlphaOfLength(10))
            .setRequired(randomBoolean())
            .setSensitive(randomBoolean())
            .setTooltip(randomAlphaOfLength(10))
            .setType(getRandomConfigurationFieldType())
            .setUiRestrictions(List.of(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            .setValidations(List.of(getRandomSettingsConfigurationValidation()))
            .setValue(randomAlphaOfLength(10))
            .build();
    }

    private static SettingsConfigurationDependency getRandomSettingsConfigurationDependency() {
        return new SettingsConfigurationDependency.Builder().setField(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static SettingsConfigurationSelectOption getRandomSettingsConfigurationSelectOption() {
        return new SettingsConfigurationSelectOption.Builder().setLabel(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static SettingsConfigurationValidation getRandomSettingsConfigurationValidation() {
        return new SettingsConfigurationValidation.Builder().setConstraint(randomAlphaOfLength(10))
            .setType(getRandomConfigurationValidationType())
            .build();
    }

    public static SettingsConfigurationDisplayType getRandomSettingsConfigurationDisplayType() {
        SettingsConfigurationDisplayType[] values = SettingsConfigurationDisplayType.values();
        return values[randomInt(values.length - 1)];
    }

    public static SettingsConfigurationFieldType getRandomConfigurationFieldType() {
        SettingsConfigurationFieldType[] values = SettingsConfigurationFieldType.values();
        return values[randomInt(values.length - 1)];
    }

    public static SettingsConfigurationValidationType getRandomConfigurationValidationType() {
        SettingsConfigurationValidationType[] values = SettingsConfigurationValidationType.values();
        return values[randomInt(values.length - 1)];
    }
}
