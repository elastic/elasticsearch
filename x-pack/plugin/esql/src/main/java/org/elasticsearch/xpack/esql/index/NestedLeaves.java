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
 * <p>
 * The parent's index holds none of these values, so nothing about them is pushed to Lucene. Full-text searches
 * ({@code MATCH}, {@code MATCH_PHRASE} and the {@code :} operator) on such a field are rejected for now: they would run as a
 * Lucene query on the parents and miss every parent whose objects match.
 * <p>
 * A field without doc values reads its object from the parent's {@code _source}. Where the mapping's {@code includes} or
 * {@code excludes} filtered {@code _source} as it was stored, it is null with a warning: filtering drops the objects it empties,
 * so the stored {@code _source} no longer tells which object each child document was indexed from.
 */
public final class NestedLeaves {
    /** Only snapshot builds read the fields under a nested object; elsewhere they are left out of the mapping. */
    public static final boolean ENABLED = Build.current().isSnapshot();

    /** The first version whose data nodes read the fields under a nested object from the child documents. */
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_flattened_leaves");

    private NestedLeaves() {}

    /**
     * Whether the fields under a nested object are read where {@code minTransportVersion} is the oldest node's version. An older
     * data node would load null for them. Only {@code IndexResolver} checks this: data nodes read the child documents only of
     * the fields it resolved as {@link org.elasticsearch.xpack.esql.core.type.EsField#isUnderNested}.
     */
    public static boolean enabled(@Nullable TransportVersion minTransportVersion) {
        return ENABLED && minTransportVersion != null && minTransportVersion.supports(VERSION);
    }

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
