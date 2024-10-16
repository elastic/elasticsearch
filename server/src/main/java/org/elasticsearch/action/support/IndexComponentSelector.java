/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * We define as index components the two different sets of indices a data stream could consist of:
 * - DATA: represents the backing indices
 * - FAILURES: represent the failing indices
 * - ALL: represents all available in this expression components, meaning if it's a data stream both backing and failure indices and if it's
 * an index only the index itself.
 * Note: An index is its own DATA component, but it cannot have a FAILURE component.
 */
public enum IndexComponentSelector implements Writeable {
    DATA("data", (byte) 0),
    FAILURES("failures", (byte) 1),
    ALL_APPLICABLE("*", (byte) 2);

    private final String key;
    private final byte id;

    IndexComponentSelector(String key, byte id) {
        this.key = key;
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public byte getId() {
        return id;
    }

    private static final Map<String, IndexComponentSelector> KEY_REGISTRY;
    private static final Map<Byte, IndexComponentSelector> ID_REGISTRY;

    static {
        Map<String, IndexComponentSelector> keyRegistry = new HashMap<>(IndexComponentSelector.values().length);
        for (IndexComponentSelector value : IndexComponentSelector.values()) {
            keyRegistry.put(value.getKey(), value);
        }
        KEY_REGISTRY = Collections.unmodifiableMap(keyRegistry);
        Map<Byte, IndexComponentSelector> idRegistry = new HashMap<>(IndexComponentSelector.values().length);
        for (IndexComponentSelector value : IndexComponentSelector.values()) {
            idRegistry.put(value.getId(), value);
        }
        ID_REGISTRY = Collections.unmodifiableMap(idRegistry);
    }

    public static IndexComponentSelector getByKey(String key) {
        IndexComponentSelector indexComponentSelector = KEY_REGISTRY.get(key);
        if (indexComponentSelector == null) {
            throw new IllegalArgumentException("Unknown index component selector [" + key + "]");
        }
        return indexComponentSelector;
    }

    public static IndexComponentSelector read(StreamInput in) throws IOException {
        var id = in.readByte();
        IndexComponentSelector indexComponentSelector = ID_REGISTRY.get(id);
        if (indexComponentSelector == null) {
            throw new IllegalArgumentException("Unknown id of index component selector [" + id + "]");
        }
        return indexComponentSelector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }
}
