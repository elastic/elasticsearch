/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.NestedObjectMapper;

import java.util.Map;

/**
 * A field under a {@code nested} object holds the values of all of the parent's objects, which Lucene stores as separate child
 * documents. ES|QL reads it as an ordinary multivalued column of the parent, like a field under a plain object.
 */
public final class NestedLeaves {
    /** Only snapshot builds read the fields under a nested object; elsewhere they are left out of the mapping. */
    public static final boolean ENABLED = Build.current().isSnapshot();

    /** The first version whose data nodes read the fields under a nested object from the child documents. */
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_flattened_leaves");

    private NestedLeaves() {}

    /** The nested object {@code name} is under in this index, from its mapping rather than from the dots in the name. */
    @Nullable
    public static String pathFor(String name, Map<String, IndexFieldCapabilities> fields) {
        String candidate = name;
        while (true) {
            IndexFieldCapabilities field = fields.get(candidate);
            if (field != null && field.type().equals(NestedObjectMapper.CONTENT_TYPE)) {
                return candidate;
            }
            int dot = candidate.lastIndexOf('.');
            if (dot < 0) {
                return null;
            }
            candidate = candidate.substring(0, dot);
        }
    }
}
