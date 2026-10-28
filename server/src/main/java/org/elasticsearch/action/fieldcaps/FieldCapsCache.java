/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.core.Nullable;

import java.util.Arrays;

/**
 * A fixed-size, direct-mapped cache for small field-caps requests with 1024 slots,
 * bounded memory usage, and no synchronization or eviction policy.
 * Collisions and races may cause cache misses but do not affect correctness.
 */
final class FieldCapsCache {
    private static final int SIZE = 1024;
    private static final int MASK = SIZE - 1;
    // These are hard-coded/small constants to keep the overhead of this cache small
    // in terms of memory usage and compute in both hit/miss paths.
    static final int MAX_FIELDS = 10;
    static final int MAX_FILTERS = 3;
    static final int MAX_INDICES = 5;

    record Key(String indexUUID, long settingsVersion, long mappingVersion, String[] fields, String[] filters) {
        int slot() {
            int h = indexUUID.hashCode();
            h = 31 * h + Arrays.hashCode(fields);
            h = 31 * h + Arrays.hashCode(filters);
            return BitMixer.mix(h) & MASK;
        }

        boolean matches(Key other) {
            return settingsVersion == other.settingsVersion
                && mappingVersion == other.mappingVersion
                && indexUUID.equals(other.indexUUID)
                && Arrays.equals(fields, other.fields)
                && Arrays.equals(filters, other.filters);
        }
    }

    private record Entry(Key key, FieldCapabilitiesIndexResponse response) {}

    private final Entry[] entries = new Entry[SIZE];

    @Nullable
    FieldCapabilitiesIndexResponse get(Key key) {
        Entry entry = entries[key.slot()];
        return entry != null && entry.key.matches(key) ? entry.response : null;
    }

    void put(Key key, FieldCapabilitiesIndexResponse resp) {
        if (resp.canMatch() && resp.get().size() <= MAX_FIELDS && resp.getMappingVersion() > 0 && resp.getIndexSettingsVersion() > 0) {
            Key responseKey = new Key(
                key.indexUUID(),
                resp.getIndexSettingsVersion(),
                resp.getMappingVersion(),
                key.fields(),
                key.filters()
            );
            entries[responseKey.slot()] = new Entry(responseKey, resp);
        }
    }
}
