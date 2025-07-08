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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class IgnoredSourceFieldLoader extends StoredFieldLoader {

    final Set<String> potentialFieldsInIgnoreSource;

    IgnoredSourceFieldLoader(StoredFieldsSpec spec) {
        Set<String> potentialFieldsInIgnoreSource = new HashSet<>();
        for (String requiredStoredField : spec.requiredStoredFields()) {
            if (requiredStoredField.startsWith(IgnoredSourceFieldMapper.NAME)) {
                String fieldName = requiredStoredField.substring(IgnoredSourceFieldMapper.NAME.length());
                potentialFieldsInIgnoreSource.addAll(FallbackSyntheticSourceBlockLoader.splitIntoFieldPaths(fieldName));
            }
        }
        this.potentialFieldsInIgnoreSource = potentialFieldsInIgnoreSource;
    }

    @Override
    public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
        var reader = sequentialReader(ctx);
        var visitor = new SFV(potentialFieldsInIgnoreSource);
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
                return null;
            }

            @Override
            public String id() {
                return null;
            }

            @Override
            public String routing() {
                return null;
            }

            @Override
            public Map<String, List<Object>> storedFields() {
                return Map.of(IgnoredSourceFieldMapper.NAME, visitor.values);
            }
        };
    }

    @Override
    public List<String> fieldsToLoad() {
        return List.of(IgnoredSourceFieldMapper.NAME);
    }

    static class SFV extends StoredFieldVisitor {

        boolean done;
        final List<Object> values = new ArrayList<>();
        final Set<String> potentialFieldsInIgnoreSource;

        SFV(Set<String> potentialFieldsInIgnoreSource) {
            this.potentialFieldsInIgnoreSource = potentialFieldsInIgnoreSource;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (done) {
                return Status.STOP;
            } else if (IgnoredSourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            } else {
                return Status.NO;
            }
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            var result = IgnoredSourceFieldMapper.decodeIfMatch(value, potentialFieldsInIgnoreSource);
            if (result != null) {
                // TODO: can't do this in case multiple entries for the same field name. (objects, arrays etc.)
//                done = true;
                values.add(result);
            }
        }

        void reset() {
            values.clear();
            done = false;
        }

    }

    static boolean supports(StoredFieldsSpec spec) {
        return spec.requiresSource() == false
            && spec.requiresMetadata() == false
            && spec.requiredStoredFields().size() == 1
            && spec.requiredStoredFields().iterator().next().startsWith(IgnoredSourceFieldMapper.NAME);
    }

    // TODO: use provided one
    private static CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> sequentialReader(LeafReaderContext ctx) throws IOException {
        LeafReader leafReader = ctx.reader();
        if (leafReader instanceof SequentialStoredFieldsLeafReader lf) {
            return lf.getSequentialStoredFieldsReader()::document;
        }
        return leafReader.storedFields()::document;
    }
}
