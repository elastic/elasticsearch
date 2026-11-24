/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The root-level aliases of the fields below passthrough objects, as the resolved mappings show them. A field below a
 * passthrough object can be named by its name below the innermost one, as a root-level field, so code that holds a field
 * by its full name and code that names it by its alias meet here. The coordinator sees no passthrough priorities, so an
 * alias here names every field that has it rather than the one a shard resolves it to.
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

    private static void collectPassthroughPaths(@Nullable String parentPath, Map<String, EsField> mapping, Set<String> passthroughPaths) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String path = parentPath == null ? entry.getKey() : parentPath + "." + entry.getKey();
            if (entry.getValue().isPassthrough()) {
                passthroughPaths.add(path);
            }
            collectPassthroughPaths(path, entry.getValue().getProperties(), passthroughPaths);
        }
    }
}
