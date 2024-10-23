/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class InferenceServiceConfigurationTestUtils {

    public static InferenceServiceConfiguration getRandomServiceConfigurationField() {
        return new InferenceServiceConfiguration.Builder().setProvider(randomAlphaOfLength(10))
            .setTaskTypes(getRandomTaskTypeEnum())
            .setConfiguration(getRandomServiceConfiguration(10))
            .build();
    }

    private static EnumSet<TaskType> getRandomTaskTypeEnum() {
        TaskType[] values = TaskType.values();
        return EnumSet.of(values[randomInt(values.length - 1)]);
    }

    private static Map<String, ServiceConfiguration> getRandomServiceConfiguration(int numFields) {
        var numConfigFields = randomInt(numFields);
        Map<String, ServiceConfiguration> configuration = new HashMap<>();
        for (int i = 0; i < numConfigFields; i++) {
            configuration.put(randomAlphaOfLength(10), ServiceConfigurationTestUtils.getRandomServiceConfigurationField());
        }

        return configuration;
    }
}
