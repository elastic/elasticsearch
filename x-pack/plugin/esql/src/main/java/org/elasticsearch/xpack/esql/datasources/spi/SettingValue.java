/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.HashMap;
import java.util.Map;

/**
 * A validated setting value with its secret classification.
 *
 * @param value the setting value as a string
 * @param isSecret whether this setting contains a credential
 */
public record SettingValue(String value, boolean isSecret) {

    /**
     * Converts a canonical settings map and field definitions into a map of SettingValues.
     */
    public static Map<String, SettingValue> fromMap(Map<String, String> canonical, Map<String, Boolean> fields) {
        Map<String, SettingValue> result = new HashMap<>();
        for (var entry : canonical.entrySet()) {
            result.put(entry.getKey(), new SettingValue(entry.getValue(), fields.getOrDefault(entry.getKey(), false)));
        }
        return result;
    }
}
