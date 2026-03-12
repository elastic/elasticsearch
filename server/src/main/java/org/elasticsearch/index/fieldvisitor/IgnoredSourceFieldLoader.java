/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class IgnoredSourceFieldLoader extends StoredFieldLoader {
    private final boolean forceSequentialReader;
    private final Map<String, Set<String>> potentialFieldsInIgnoreSource;
    private final Set<String> fieldNames;

    IgnoredSourceFieldLoader(StoredFieldsSpec spec, boolean forceSequentialReader) {
        assert IgnoredSourceFieldLoader.supports(spec);

        fieldNames = new HashSet<>(spec.sourcePaths());
        this.forceSequentialReader = forceSequentialReader;

        HashMap<String, Set<String>> potentialFieldsInIgnoreSource = new HashMap<>();
        for (String requiredIgnoredField : spec.sourcePaths()) {
            for (String potentialIgnoredField : FallbackSyntheticSourceBlockLoader.splitIntoFieldPaths(requiredIgnoredField)) {
                potentialFieldsInIgnoreSource.computeIfAbsent(potentialIgnoredField, k -> new HashSet<>()).add(requiredIgnoredField);
            }
        }
        this.potentialFieldsInIgnoreSource = potentialFieldsInIgnoreSource;
    }

    @Override
    public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
        var reader = forceSequentialReader ? sequentialReader(ctx) : reader(ctx, docs);
        var visitor = new SFV(fieldNames, potentialFieldsInIgnoreSource);
        return new LeafStoredFieldLoader() {

            private int doc = -1;

            @Override
            public void advanceTo(int doc) throws IOException {
                if (doc != this.doc) {
                    visitor.reset();
                    reader.accept(doc, visitor);
                    this.doc = doc;
                }
            }

            @Override
            public BytesReference source() {
                assert false : "source() is not supported by IgnoredSourceFieldLoader";
                return null;
            }

            @Override
            public String id() {
                assert false : "id() is not supported by IgnoredSourceFieldLoader";
                return null;
            }

            @Override
            public String routing() {
                assert false : "routing() is not supported by IgnoredSourceFieldLoader";
                return null;
            }

            @Override
            public Map<String, List<Object>> storedFields() {
                return Map.of(IgnoredSourceFieldMapper.NAME, Collections.unmodifiableList(visitor.values));
            }
        };
    }

    @Override
    public List<String> fieldsToLoad() {
        return potentialFieldsInIgnoreSource.keySet().stream().toList();
    }

    static class SFV extends StoredFieldVisitor {
        List<List<IgnoredSourceFieldMapper.NameValue>> values = new ArrayList<>();
        final Set<String> fieldNames;
        private final Set<String> unvisitedFields;
        final Map<String, Set<String>> potentialFieldsInIgnoreSource;

        SFV(Set<String> fieldNames, Map<String, Set<String>> potentialFieldsInIgnoreSource) {
            this.fieldNames = fieldNames;
            this.unvisitedFields = new HashSet<>(fieldNames);
            this.potentialFieldsInIgnoreSource = potentialFieldsInIgnoreSource;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (unvisitedFields.isEmpty()) {
                return Status.STOP;
            }

            if (fieldInfo.name.equals(IgnoredSourceFieldMapper.NAME)) {
                return Status.YES;
            }

            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            var nameValues = IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.decode(new BytesRef(value));
            assert nameValues.isEmpty() == false;
            String fieldPath = nameValues.getFirst().name();

            Set<String> foundValues = potentialFieldsInIgnoreSource.get(fieldPath);
            if (foundValues == null) {
                return;
            }

            unvisitedFields.removeAll(foundValues);
            values.add(nameValues);
        }

        void reset() {
            values.clear();
            unvisitedFields.addAll(fieldNames);
        }

    }

    static boolean supports(StoredFieldsSpec spec) {
        return spec.onlyRequiresSourcePaths()
            && spec.ignoredSourceFormat() == IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE;
    }
}
