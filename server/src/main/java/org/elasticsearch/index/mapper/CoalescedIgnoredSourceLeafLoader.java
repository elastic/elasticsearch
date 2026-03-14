/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceLeafLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.NameValue;
import org.elasticsearch.search.lookup.SourceFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link IgnoredSourceLeafLoader} for the {@link IgnoredSourceFieldMapper.IgnoredSourceFormat#COALESCED_SINGLE_IGNORED_SOURCE} format.
 * All values for a given field path are grouped into a single stored field entry, encoded via {@link CoalescedIgnoredSourceEncoding}.
 * Each entry is a binary blob with the format: {@code [count][field_name][parentOffset1][value1][parentOffset2][value2]...}
 *
 * <p>For example, a document with:
 * <pre>{@code
 *   { "obj": { "foo": "a", "foo": "b" }, "bar": 42 }
 * }</pre>
 * where {@code obj.foo} and {@code bar} are ignored, would produce two stored field entries under {@code _ignored_source}:
 * <ul>
 *   <li>{@code [2]["obj.foo"][4]["a"][4]["b"]} — count=2, field name, then two (parentOffset, value) pairs</li>
 *   <li>{@code [1]["bar"][0][42]} — count=1, field name, then one (parentOffset, value) pair</li>
 * </ul>
 */
final class CoalescedIgnoredSourceLeafLoader implements IgnoredSourceLeafLoader {

    @Override
    public Map<String, List<NameValue>> loadAllIgnoredFields(SourceFilter filter, Map<String, List<Object>> storedFields, int docId) {
        var ignoredStoredValues = storedFields.get(IgnoredSourceFieldMapper.NAME);
        if (ignoredStoredValues == null) {
            return Map.of();
        }

        Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
        for (var ignoredSourceEntry : ignoredStoredValues) {
            List<NameValue> nameValues = decodeEntry(ignoredSourceEntry);

            // Filter out entries excluded by the source filter and group the rest by parent field name.
            for (var nameValue : nameValues) {
                if (filter != null && filter.isPathFiltered(nameValue.name(), XContentDataHelper.isEncodedObject(nameValue.value()))) {
                    continue;
                }
                objectsWithIgnoredFields.computeIfAbsent(nameValue.getParentFieldName(), k -> new ArrayList<>()).add(nameValue);
            }
        }
        return objectsWithIgnoredFields;
    }

    @Override
    public Map<String, List<NameValue>> loadSingleIgnoredField(Set<String> fieldPaths, Map<String, List<Object>> storedFields, int docId) {
        var ignoredStoredValues = storedFields.get(IgnoredSourceFieldMapper.NAME);
        if (ignoredStoredValues == null) {
            return Map.of();
        }

        Map<String, List<NameValue>> valuesForFieldAndParents = new HashMap<>();
        for (var ignoredSourceEntry : ignoredStoredValues) {
            List<NameValue> nameValues = decodeEntry(ignoredSourceEntry);

            // All values in a coalesced entry share the same field name; check if it matches one of the requested paths.
            String fieldPath = nameValues.getFirst().name();
            if (fieldPaths.contains(fieldPath)) {
                // Each field path appears in at most one coalesced entry.
                assert valuesForFieldAndParents.containsKey(fieldPath) == false;
                valuesForFieldAndParents.put(fieldPath, nameValues);
            }
        }
        return valuesForFieldAndParents;
    }

    /**
     * Decodes a single coalesced entry.
     */
    @SuppressWarnings("unchecked")
    private static List<NameValue> decodeEntry(Object ignoredSourceEntry) {
        // The entry is either already decoded (NameValue list) or a BytesRef that needs to be decoded
        List<NameValue> nameValues = (ignoredSourceEntry instanceof List<?>)
            ? (List<NameValue>) ignoredSourceEntry
            : CoalescedIgnoredSourceEncoding.decode((BytesRef) ignoredSourceEntry);
        assert nameValues.isEmpty() == false;
        return nameValues;
    }
}
