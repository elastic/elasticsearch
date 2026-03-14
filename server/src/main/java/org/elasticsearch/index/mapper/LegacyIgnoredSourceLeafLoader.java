/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceLeafLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.LegacyIgnoredSourceEncoding;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.NameValue;
import org.elasticsearch.search.lookup.SourceFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link IgnoredSourceLeafLoader} for the {@link IgnoredSourceFieldMapper.IgnoredSourceFormat#LEGACY_SINGLE_IGNORED_SOURCE} format.
 * Each ignored-source entry is stored as a separate stored field under {@code _ignored_source}, encoded via
 * {@link LegacyIgnoredSourceEncoding}.
 *
 * <p>For example, a document with:
 * <pre>{@code
 *   { "obj": { "foo": "a" }, "bar": 42 }
 * }</pre>
 * where {@code obj.foo} and {@code bar} are ignored, would produce two separate stored field entries under
 * {@code _ignored_source}, one for each ignored value.
 */
final class LegacyIgnoredSourceLeafLoader implements IgnoredSourceLeafLoader {

    @Override
    public Map<String, List<NameValue>> loadAllIgnoredFields(SourceFilter filter, Map<String, List<Object>> storedFields, int docId) {
        var ignoredStoredValues = storedFields.get(IgnoredSourceFieldMapper.NAME);
        if (ignoredStoredValues == null) {
            return Map.of();
        }

        Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
        for (Object value : ignoredStoredValues) {
            NameValue nv = LegacyIgnoredSourceEncoding.decode(value);

            // Skip entries excluded by the source filter.
            if (filter != null && filter.isPathFiltered(nv.name(), XContentDataHelper.isEncodedObject(nv.value()))) {
                continue;
            }

            // Group by parent field name so the caller can reconstruct each object.
            objectsWithIgnoredFields.computeIfAbsent(nv.getParentFieldName(), k -> new ArrayList<>()).add(nv);
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
        for (Object value : ignoredStoredValues) {
            NameValue nv = LegacyIgnoredSourceEncoding.decode(value);

            // Collect only entries matching one of the requested field paths.
            if (fieldPaths.contains(nv.name())) {
                valuesForFieldAndParents.computeIfAbsent(nv.name(), k -> new ArrayList<>()).add(nv);
            }
        }
        return valuesForFieldAndParents;
    }
}
