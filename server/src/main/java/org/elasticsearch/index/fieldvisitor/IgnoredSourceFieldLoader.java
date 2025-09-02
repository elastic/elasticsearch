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
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
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

        fieldNames = new HashSet<>(spec.ignoredFieldsSpec().requiredIgnoredFields());
        this.forceSequentialReader = forceSequentialReader;

        HashMap<String, Set<String>> potentialFieldsInIgnoreSource = new HashMap<>();
        for (String requiredIgnoredField : spec.ignoredFieldsSpec().requiredIgnoredFields()) {
            for (String potentialStoredField : spec.ignoredFieldsSpec().format().requiredStoredFields(requiredIgnoredField)) {
                potentialFieldsInIgnoreSource.computeIfAbsent(potentialStoredField, k -> new HashSet<>()).add(requiredIgnoredField);
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
                return visitor.values;
            }
        };
    }

    @Override
    public List<String> fieldsToLoad() {
        return potentialFieldsInIgnoreSource.keySet().stream().toList();
    }

    static class SFV extends StoredFieldVisitor {
        final Map<String, List<Object>> values = new HashMap<>();
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

            Set<String> foundFields = potentialFieldsInIgnoreSource.get(fieldInfo.name);
            if (foundFields == null) {
                return Status.NO;
            }

            unvisitedFields.removeAll(foundFields);
            return Status.YES;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            values.computeIfAbsent(fieldInfo.name, k -> new ArrayList<>()).add(new BytesRef(value));
        }

        void reset() {
            values.clear();
            unvisitedFields.addAll(fieldNames);
        }

    }

    static boolean supports(StoredFieldsSpec spec) {
        return spec.onlyRequiresIgnoredFields()
            && spec.ignoredFieldsSpec().format() == IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE;
    }
}
