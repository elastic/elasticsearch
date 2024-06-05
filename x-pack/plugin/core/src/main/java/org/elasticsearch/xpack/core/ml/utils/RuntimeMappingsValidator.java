/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import java.util.HashMap;
import java.util.Map;

public final class RuntimeMappingsValidator {

    /**
     * Perform a light check that the structure resembles runtime_mappings.
     * The full check cannot happen until search
     */
    public static void validate(Map<String, Object> runtimeMappings) {
        for (Map.Entry<String, Object> entry : runtimeMappings.entrySet()) {
            // top level objects are fields
            String fieldName = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propNode = new HashMap<>(((Map<String, Object>) entry.getValue()));
                Object typeNode = propNode.get("type");
                if (typeNode == null) {
                    throw ExceptionsHelper.badRequestException("No type specified for runtime field [{}]", fieldName);
                }
            } else {
                throw ExceptionsHelper.badRequestException(
                    "Expected map for runtime field [{}] definition but got a {}",
                    fieldName,
                    fieldName.getClass().getSimpleName()
                );
            }
        }
    }
}
