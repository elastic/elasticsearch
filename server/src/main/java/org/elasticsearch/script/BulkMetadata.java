/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/** Common operations for metadata updates done on bulk search operations: reindex and update by query */
public class BulkMetadata extends Metadata {
    protected static final FieldProperty<String> RO_STRING = new FieldProperty<>(String.class, false, false, null);
    protected static final FieldProperty<String> RO_NULLABLE_STRING = new FieldProperty<>(String.class, true, false, null);
    protected static final FieldProperty<Number> RO_LONG = new FieldProperty<>(Number.class, false, false, FieldProperty.LONGABLE_NUMBER);

    protected static final FieldProperty<Number> RW_NULLABLE_LONG = new FieldProperty<>(
        Number.class,
        false,
        false,
        FieldProperty.LONGABLE_NUMBER
    );
    protected static Metadata.FieldProperty<String> RW_NULLABLE_STRING = new FieldProperty<>(String.class, true, true, null);
    protected static Metadata.FieldProperty<String> RW_STRING = new FieldProperty<>(String.class, false, true, null);

    protected static final FieldProperty<String> OP_PROPERTY = new FieldProperty<>(
        String.class,
        false,
        true,
        setValidator(Set.of("noop", "index", "delete"))
    );

    protected static BiConsumer<String, String> setValidator(Set<String> valid) {
        return (k, v) -> {
            if (valid.contains(v) == false) {
                throw new IllegalArgumentException(
                    "[" + k + "] must be one of " + valid.stream().sorted().collect(Collectors.joining(", ")) + ", not [" + v + "]"
                );
            }
        };
    }

    /**
     * Reindex: _index rw, non-null
     *          _id rw, null
     *          _version, rw null
     *          _routing, rw null
     *          _op, rw 'index', 'noop', 'delete'
     * UpdateByQuery:
     *          _index, ro, non-null
     *          _id, ro, non-null
     *          _version, ro, non-null
     *          _routing, ro, null
     *          _op, rw 'index', 'noop', 'delete'
     */
    protected static Map<String, Object> metadataMap(String index, String id, Long version, String routing, String op, long timestamp) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(6);
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        metadata.put(OP, op);
        metadata.put(TIMESTAMP, timestamp);
        return metadata;
    }

    protected BulkMetadata(
        String index,
        String id,
        Long version,
        String routing,
        String op,
        long timestamp,
        Map<String, FieldProperty<?>> properties
    ) {
        super(metadataMap(index, id, version, routing, op, timestamp), properties);
    }
}
