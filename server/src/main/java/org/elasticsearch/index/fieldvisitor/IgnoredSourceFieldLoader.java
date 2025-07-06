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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class IgnoredSourceFieldLoader extends StoredFieldLoader {

    @Override
    public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
        var reader = sequentialReader(ctx);
        var visitor = new SFV();
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

        boolean processing;
        final List<Object> values = new ArrayList<>();

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (IgnoredSourceFieldMapper.NAME.equals(fieldInfo.name)) {
                processing = true;
                return Status.YES;
            } else if (processing) {
                return Status.STOP;
            }

            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            values.add(new BytesRef(value));
        }

        void reset() {
            values.clear();
            processing = false;
        }

    }

    static boolean supports(StoredFieldsSpec spec) {
        return spec.requiresSource() == false
            && spec.requiresMetadata() == false
            && spec.requiredStoredFields().size() == 1
            && spec.requiredStoredFields().contains(IgnoredSourceFieldMapper.NAME);
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
