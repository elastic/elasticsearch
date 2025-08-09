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

public class MappingPredicates {

    private final Mapping mapping;

    public MappingPredicates(Mapping mapping) {
        this.mapping = mapping;
    }

    record PathMapping(String path, Map<String, Object> mapping) {}

    private List<PathMapping> getPathMapping(String path) {
        String[] parts = path.split("\\.");
        var result = new ArrayList<PathMapping>();
        for (int i = 0; i < parts.length; i++) {
            var pathToHere = String.join(".", Arrays.copyOfRange(parts, 0, i + 1));
            Map<String, Object> fieldMapping = mapping.lookup().get(pathToHere);
            if (fieldMapping == null) {
                break;
            }
            result.add(new PathMapping(pathToHere, fieldMapping));
        }
        return result;
    }

    public List<String> getNestedPathPrefixes(String fullPath) {
        return getPathMapping(fullPath).stream().filter(pm -> "nested".equals(pm.mapping().get("type"))).map(PathMapping::path).toList();
    }

    public boolean inNestedContext(String fullPath) {
        return getPathMapping(fullPath).stream().anyMatch(pm -> "nested".equals(pm.mapping().get("type")));
    }

    @SuppressWarnings("unchecked")
    public boolean isRuntimeField(String path) {
        var topLevelMapping = (Map<String, Object>) mapping.raw().get("_doc");
        boolean inRuntimeContext = "runtime".equals(topLevelMapping.get("dynamic"));
        for (var pm : getPathMapping(path)) {
            if (pm.mapping().containsKey("dynamic")) {
                // lower down dynamic definitions override higher up behavior
                inRuntimeContext = "runtime".equals(pm.mapping().get("dynamic"));
            }
        }
        return inRuntimeContext;
    }

}
