/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.inference.configuration.ServiceConfigurationDependency;
import org.elasticsearch.inference.configuration.ServiceConfigurationDisplayType;
import org.elasticsearch.inference.configuration.ServiceConfigurationFieldType;
import org.elasticsearch.inference.configuration.ServiceConfigurationSelectOption;
import org.elasticsearch.inference.configuration.ServiceConfigurationValidation;
import org.elasticsearch.inference.configuration.ServiceConfigurationValidationType;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class ServiceConfigurationTestUtils {

    public static ServiceConfiguration getRandomServiceConfigurationField() {
        return new ServiceConfiguration.Builder().setCategory(randomAlphaOfLength(10))
            .setDefaultValue(randomAlphaOfLength(10))
            .setDependsOn(List.of(getRandomServiceConfigurationDependency()))
            .setDisplay(getRandomServiceConfigurationDisplayType())
            .setLabel(randomAlphaOfLength(10))
            .setOptions(List.of(getRandomServiceConfigurationSelectOption(), getRandomServiceConfigurationSelectOption()))
            .setOrder(randomInt())
            .setPlaceholder(randomAlphaOfLength(10))
            .setRequired(randomBoolean())
            .setSensitive(randomBoolean())
            .setTooltip(randomAlphaOfLength(10))
            .setType(getRandomConfigurationFieldType())
            .setUiRestrictions(List.of(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            .setValidations(List.of(getRandomServiceConfigurationValidation()))
            .setValue(randomAlphaOfLength(10))
            .build();
    }

    private static ServiceConfigurationDependency getRandomServiceConfigurationDependency() {
        return new ServiceConfigurationDependency.Builder().setField(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static ServiceConfigurationSelectOption getRandomServiceConfigurationSelectOption() {
        return new ServiceConfigurationSelectOption.Builder().setLabel(randomAlphaOfLength(10)).setValue(randomAlphaOfLength(10)).build();
    }

    private static ServiceConfigurationValidation getRandomServiceConfigurationValidation() {
        return new ServiceConfigurationValidation.Builder().setConstraint(randomAlphaOfLength(10))
            .setType(getRandomConfigurationValidationType())
            .build();
    }

    public static ServiceConfigurationDisplayType getRandomServiceConfigurationDisplayType() {
        ServiceConfigurationDisplayType[] values = ServiceConfigurationDisplayType.values();
        return values[randomInt(values.length - 1)];
    }

    public static ServiceConfigurationFieldType getRandomConfigurationFieldType() {
        ServiceConfigurationFieldType[] values = ServiceConfigurationFieldType.values();
        return values[randomInt(values.length - 1)];
    }

    public static ServiceConfigurationValidationType getRandomConfigurationValidationType() {
        ServiceConfigurationValidationType[] values = ServiceConfigurationValidationType.values();
        return values[randomInt(values.length - 1)];
    }
}
