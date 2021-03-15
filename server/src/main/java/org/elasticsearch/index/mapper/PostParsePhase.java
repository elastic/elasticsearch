/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Executes post parse phases on mappings
 */
public class PostParsePhase {

    private final Map<String, OneTimeFieldExecutor> fieldExecutors = new HashMap<>();
    private final PostParseContext context;

    /**
     * Given a mapping, collects all {@link PostParseExecutor}s and executes them
     * @param lookup        the MappingLookup to collect executors from
     * @param parseContext  the ParseContext of the current document
     * @throws IOException
     */
    public static void executePostParsePhases(MappingLookup lookup, ParseContext parseContext) throws IOException {
        if (lookup.getPostParseExecutors().isEmpty()) {
            return;
        }
        PostParsePhase postParsePhase = new PostParsePhase(lookup, parseContext);
        postParsePhase.executePostParse();
    }

    private PostParsePhase(MappingLookup mappingLookup, ParseContext pc) {
        LazyDocumentReader reader = new LazyDocumentReader(
            pc.rootDoc(),
            pc.sourceToParse().source(),
            mappingLookup.getPostParseExecutors().keySet());
        this.context = new PostParseContext(mappingLookup, pc, reader.getContext());
        mappingLookup.getPostParseExecutors().forEach((k, c) -> fieldExecutors.put(k, new OneTimeFieldExecutor(c)));
    }

    private void executePostParse() throws IOException {
        for (OneTimeFieldExecutor ms : fieldExecutors.values()) {
            ms.execute();
        }
    }

    private void executeField(String field) throws IOException {
        assert fieldExecutors.containsKey(field);
        fieldExecutors.get(field).execute();
    }

    // FieldExecutors can be called both by executePostParse() and from the lazy reader,
    // so to ensure that we don't run field scripts multiple times we guard them with
    // this one-time executor class
    private class OneTimeFieldExecutor {

        PostParseExecutor executor;
        boolean executed = false;

        OneTimeFieldExecutor(PostParseExecutor executor) {
            this.executor = executor;
        }

        void execute() throws IOException {
            if (executed == false) {
                executor.execute(context);
                executed = true;
            }
        }
    }

    private class LazyDocumentReader extends LeafReader {

        private final ParseContext.Document document;
        private final BytesReference sourceBytes;
        private final Set<String> calculatedFields;
        private final Set<String> fieldPath = new LinkedHashSet<>();
        private LeafReaderContext in;

        private LazyDocumentReader(ParseContext.Document document, BytesReference sourceBytes, Set<String> calculatedFields) {
            this.document = document;
            this.sourceBytes = sourceBytes;
            this.calculatedFields = calculatedFields;
        }

        private void buildDocValues(String field) throws IOException {
            if (calculatedFields.contains(field)) {
                // this means that a mapper script is referring to another calculated field;
                // in which case we need to execute that field first, and then rebuild the
                // memory index.  We also check for loops here
                if (fieldPath.add(field) == false) {
                    throw new IllegalStateException("Loop in field resolution detected: " + String.join("->", fieldPath) + "->" + field);
                }
                executeField(field);
                calculatedFields.remove(field);
                fieldPath.remove(field);
                this.in = null;
            }
            if (in != null) {
                return;
            }
            MemoryIndex mi = new MemoryIndex();
            for (IndexableField f : document) {
                if (f.fieldType().docValuesType() != null) {
                    mi.addField(f, EMPTY_ANALYZER);
                }
            }
            mi.freeze();
            this.in = mi.createSearcher().getIndexReader().leaves().get(0);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            buildDocValues(field);
            return in.reader().getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            buildDocValues(field);
            return in.reader().getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            buildDocValues(field);
            return in.reader().getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            buildDocValues(field);
            return in.reader().getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            buildDocValues(field);
            return in.reader().getSortedSetDocValues(field);
        }

        @Override
        public FieldInfos getFieldInfos() {
            try {
                buildDocValues(null);
            } catch (IOException e) {
                // won't actually happen
            }
            return in.reader().getFieldInfos();
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            visitor.binaryField(SOURCE_FIELD_INFO, sourceBytes.toBytesRef().bytes);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Terms terms(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getLiveDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeafMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Fields getTermVectors(int docID) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int maxDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }
    }

    private static final FieldInfo SOURCE_FIELD_INFO = new FieldInfo(
        "_source",
        0,
        false,
        false,
        false,
        IndexOptions.NONE,
        DocValuesType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        false
    );

    private static final Analyzer EMPTY_ANALYZER = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(reader -> {}, null);
        }
    };
}
