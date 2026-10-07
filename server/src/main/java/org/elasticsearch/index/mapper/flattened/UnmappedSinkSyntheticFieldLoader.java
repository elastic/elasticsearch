/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Synthetic source loader for the implicit {@code _unmapped} sink. Absorbed fields must render as if they had never been absorbed, so the
 * sink is never written under its own name: the enclosing object loader (the root, or a nested object) pulls the values out through
 * {@link #valuesByKey()} and writes each key under its original full dotted path relative to that object, sorted among the mapped fields
 * and source-filtered per key.
 */
public final class UnmappedSinkSyntheticFieldLoader extends FlattenedDocValuesSyntheticFieldLoader {

    UnmappedSinkSyntheticFieldLoader(
        String fieldFullPath,
        @Nullable String keyedIgnoredValuesFieldFullPath,
        boolean storeIgnoredFieldsInBinaryDocValues
    ) {
        super(
            fieldFullPath,
            fieldFullPath + FlattenedFieldMapper.KEYED_FIELD_SUFFIX,
            keyedIgnoredValuesFieldFullPath,
            fieldFullPath,
            true,
            List.of(),
            storeIgnoredFieldsInBinaryDocValues,
            FlattenedFieldMapper.PreserveLeafArrays.EXACT,
            true
        );
    }

    /**
     * The current document's absorbed values keyed by full dotted path. Each list keeps document order, duplicates and nulls, with
     * ignore_above values appended last.
     */
    public Map<String, List<String>> valuesByKey() {
        Map<String, List<String>> values = new HashMap<>();
        try {
            var producer = getKeyedValueProducer();
            for (var field = producer.next(); field != null; field = producer.next()) {
                values.put(field.key().fullPath(), field.values());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return values;
    }

    /** Writes one absorbed key and its values, as a scalar for a single value and as an array otherwise. */
    public static void writeKey(XContentBuilder b, String key, List<String> values) throws IOException {
        FlattenedFieldSyntheticWriterHelper.writeField(b, values, key);
    }
}
