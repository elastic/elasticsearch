/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The root-level aliases of the fields below passthrough objects, as the resolved mappings show them. A field below a
 * passthrough object can be named by its name below the innermost one, as a root-level field, so code that holds a field
 * by its full name and code that names it by its alias meet here.
 */
public final class PassthroughFields {
    private PassthroughFields() {}

    /** The paths of the passthrough objects in the mappings of the valid {@code resolutions}. */
    public static Set<String> passthroughPaths(Collection<IndexResolution> resolutions) {
        Set<String> paths = new HashSet<>();
        for (IndexResolution resolution : resolutions) {
            if (resolution != null && resolution.isValid()) {
                collectPassthroughPaths(null, resolution.get().mapping(), paths);
            }
        }
        return paths;
    }

    /**
     * The field name relative to its innermost passthrough parent, which is the name of its root-level alias.
     */
    public static String withoutPassthroughParents(String fieldName, Set<String> passthroughPaths) {
        for (int dot = fieldName.lastIndexOf('.'); dot > 0; dot = fieldName.lastIndexOf('.', dot - 1)) {
            if (passthroughPaths.contains(fieldName.substring(0, dot))) {
                return fieldName.substring(dot + 1);
            }
        }
        return fieldName;
    }

    /** The fields below a passthrough object in the mappings of the valid {@code resolutions}, keyed by their alias. */
    public static Map<String, Set<String>> fieldsByAlias(Collection<IndexResolution> resolutions) {
        Set<String> passthroughPaths = passthroughPaths(resolutions);
        Map<String, Set<String>> fields = new HashMap<>();
        for (IndexResolution resolution : resolutions) {
            if (resolution != null && resolution.isValid()) {
                collectFieldsByAlias(null, resolution.get().mapping(), passthroughPaths, fields);
            }
        }
        return fields;
    }

    private static void collectPassthroughPaths(@Nullable String parentPath, Map<String, EsField> mapping, Set<String> passthroughPaths) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String path = parentPath == null ? entry.getKey() : parentPath + "." + entry.getKey();
            if (entry.getValue().isPassthrough()) {
                passthroughPaths.add(path);
            }
            collectPassthroughPaths(path, entry.getValue().getProperties(), passthroughPaths);
        }
    }

    private static void collectFieldsByAlias(
        @Nullable String parentPath,
        Map<String, EsField> mapping,
        Set<String> passthroughPaths,
        Map<String, Set<String>> fields
    ) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String path = parentPath == null ? entry.getKey() : parentPath + "." + entry.getKey();
            if (entry.getValue().getDataType() != DataType.OBJECT) {
                String alias = withoutPassthroughParents(path, passthroughPaths);
                if (alias.equals(path) == false) {
                    fields.computeIfAbsent(alias, k -> new TreeSet<>()).add(path);
                }
            }
            collectFieldsByAlias(path, entry.getValue().getProperties(), passthroughPaths, fields);
        }
    }
}
