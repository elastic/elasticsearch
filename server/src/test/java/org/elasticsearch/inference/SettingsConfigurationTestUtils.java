/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;

import java.util.EnumSet;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class SettingsConfigurationTestUtils {

    public static SettingsConfiguration getRandomSettingsConfigurationField() {
        return new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)).setDefaultValue(
            randomAlphaOfLength(10)
        )
            .setDescription(randomAlphaOfLength(10))
            .setLabel(randomAlphaOfLength(10))
            .setRequired(randomBoolean())
            .setSensitive(randomBoolean())
            .setUpdatable(randomBoolean())
            .setType(getRandomConfigurationFieldType())
            .build();
    }

    public static SettingsConfigurationFieldType getRandomConfigurationFieldType() {
        SettingsConfigurationFieldType[] values = SettingsConfigurationFieldType.values();
        return values[randomInt(values.length - 1)];
    }
}
