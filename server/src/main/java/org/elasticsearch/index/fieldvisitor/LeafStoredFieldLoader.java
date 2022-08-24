/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class LeafStoredFieldLoader {

    public abstract void advanceTo(int doc) throws IOException;

    public abstract BytesReference source();

    public abstract String id();

    public abstract String routing();

    public abstract Map<String, List<Object>> storedFields();

    public static LeafStoredFieldLoader forDocs(LeafReaderContext ctx, int[] docs, boolean loadSource, Set<String> fields) {
        return new ReaderStoredFieldLoader(reader(ctx, docs), loadSource, fields);
    }

    public static LeafStoredFieldLoader forContext(LeafReaderContext ctx, boolean loadSource, Set<String> fields) {
        return new ReaderStoredFieldLoader(ctx.reader()::document, loadSource, fields);
    }

    public static LeafStoredFieldLoader empty() {
        return new EmptyStoredFieldLoader();
    }

    private static CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader(LeafReaderContext ctx, int[] docs) {
        LeafReader leafReader = ctx.reader();
        if (leafReader instanceof SequentialStoredFieldsLeafReader lf && docs.length > 10 && hasSequentialDocs(docs)) {
            return lf.getSequentialStoredFieldsReader()::visitDocument;
        }
        return leafReader::document;
    }

    private static boolean hasSequentialDocs(int[] docs) {
        return docs.length > 0 && docs[docs.length - 1] - docs[0] == docs.length - 1;
    }

    private static class EmptyStoredFieldLoader extends LeafStoredFieldLoader {

        @Override
        public void advanceTo(int doc) throws IOException {

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
            return Collections.emptyMap();
        }
    }

    private static class ReaderStoredFieldLoader extends LeafStoredFieldLoader {

        private final CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader;
        private final CustomFieldsVisitor visitor;
        private int doc;

        ReaderStoredFieldLoader(CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader, boolean loadSource, Set<String> fields) {
            this.reader = reader;
            this.visitor = new CustomFieldsVisitor(fields, loadSource);
        }

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
            return visitor.source();
        }

        @Override
        public String id() {
            return visitor.id();
        }

        @Override
        public String routing() {
            return visitor.routing();
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return visitor.fields();
        }

    }

}
