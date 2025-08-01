/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.queries;

import org.elasticsearch.datageneration.Mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MappingContextHelper {

    private final Mapping mapping;

    public MappingContextHelper(Mapping mapping) {
        this.mapping = mapping;
    }

    @SuppressWarnings("unchecked")
    public List<String> getNestedPathPrefixes(String[] path) {
        Map<String, Object> mapping = this.mapping.raw();
        mapping = (Map<String, Object>) mapping.get("_doc");
        mapping = (Map<String, Object>) mapping.get("properties");

        var result = new ArrayList<String>();
        for (int i = 0; i < path.length - 1; i++) {
            var field = path[i];
            mapping = (Map<String, Object>) mapping.get(field);

            // dynamic field
            if (mapping == null) {
                break;
            }

            boolean nested = "nested".equals(mapping.get("type"));
            if (nested) {
                result.add(String.join(".", Arrays.copyOfRange(path, 0, i + 1)));
            }
            mapping = (Map<String, Object>) mapping.get("properties");
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public boolean isRuntimeField(String path) {
        String[] parts = path.split("\\.");
        var topLevelMapping = (Map<String, Object>) mapping.raw().get("_doc");
        boolean inRuntimeContext = "runtime".equals(topLevelMapping.get("dynamic"));
        for (int i = 0; i < parts.length - 1; i++) {
            var pathToHere = String.join(".", Arrays.copyOfRange(parts, 0, i + 1));
            Map<String, Object> fieldMapping = mapping.lookup().get(pathToHere);
            if (fieldMapping == null) {
                break;
            }
            if (fieldMapping.containsKey("dynamic")) {
                // lower down dynamic definitions override higher up behavior
                inRuntimeContext = "runtime".equals(fieldMapping.get("dynamic"));
            }
        }
        return inRuntimeContext;
    }

    public boolean inNestedContext(String path) {
        return getNestedPathPrefixes(path.split("\\.")).isEmpty() == false;
    }
}
