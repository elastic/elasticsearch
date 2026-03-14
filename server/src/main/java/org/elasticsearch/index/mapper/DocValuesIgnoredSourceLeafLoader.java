/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceLeafLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.LegacyIgnoredSourceEncoding;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.NameValue;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link IgnoredSourceLeafLoader} for the {@link IgnoredSourceFieldMapper.IgnoredSourceFormat#DOC_VALUES_IGNORED_SOURCE} format.
 * Each document's ignored-source entries are stored in a single binary doc value with a two-layer encoding:
 *
 * <p><b>Outer layer</b> — {@link MultiValuedBinaryDocValuesField.IntegratedCount} format:
 * <pre>{@code [count][len1][entry1][len2][entry2]...}</pre>
 * where {@code count} is the number of entries and each {@code entry} is a {@link LegacyIgnoredSourceEncoding}-encoded blob.
 *
 * <p><b>Inner layer</b> — each entry uses {@link LegacyIgnoredSourceEncoding}:
 * <pre>{@code [nameLength + parentOffset * 65536 (4 bytes LE)][nameBytes][valueBytes]}</pre>
 *
 * <p>For example, a document with:
 * <pre>{@code
 *   { "obj": { "foo": "a" }, "bar": 42 }
 * }</pre>
 * where {@code obj.foo} and {@code bar} are ignored, would produce a single binary doc value containing two
 * {@link LegacyIgnoredSourceEncoding} entries wrapped in the {@code IntegratedCount} format:
 * <pre>{@code [2][len]["obj.foo" entry][len]["bar" entry]}</pre>
 */
final class DocValuesIgnoredSourceLeafLoader implements IgnoredSourceLeafLoader {

    private final MultiValuedSortedBinaryDocValues docValues;

    DocValuesIgnoredSourceLeafLoader(LeafReader leafReader) throws IOException {
        this.docValues = MultiValuedSortedBinaryDocValues.from(leafReader, IgnoredSourceFieldMapper.NAME);
    }

    @Override
    public Map<String, List<NameValue>> loadAllIgnoredFields(SourceFilter filter, Map<String, List<Object>> storedFields, int docId)
        throws IOException {
        // Advance doc values to the target document
        if (docValues == null || docValues.advanceExact(docId) == false) {
            return Map.of();
        }

        Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
        int count = docValues.docValueCount();

        for (int i = 0; i < count; i++) {
            NameValue nv = LegacyIgnoredSourceEncoding.decode(docValues.nextValue());

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
    public Map<String, List<NameValue>> loadSingleIgnoredField(Set<String> fieldPaths, Map<String, List<Object>> storedFields, int docId)
        throws IOException {
        // Advance doc values to the target document
        if (docValues == null || docValues.advanceExact(docId) == false) {
            return Map.of();
        }

        Map<String, List<NameValue>> valuesForFieldAndParents = new HashMap<>();
        int count = docValues.docValueCount();

        for (int i = 0; i < count; i++) {
            NameValue nv = LegacyIgnoredSourceEncoding.decode(docValues.nextValue());

            // Collect only entries matching one of the requested field paths.
            if (fieldPaths.contains(nv.name())) {
                valuesForFieldAndParents.computeIfAbsent(nv.name(), k -> new ArrayList<>()).add(nv);
            }
        }
        return valuesForFieldAndParents;
    }
}
