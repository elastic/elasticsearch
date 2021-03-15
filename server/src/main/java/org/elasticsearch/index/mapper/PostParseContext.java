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
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PostParseContext {

    public final SearchLookup searchLookup;
    public final LeafReaderContext leafReaderContext;
    public final ParseContext pc;
    private final Map<String, FieldExecutor> fieldExecutors = new HashMap<>();

    public static void executePostParsePhases(MappingLookup lookup, ParseContext parseContext) throws IOException {
        if (lookup.getPostParsePhases().isEmpty()) {
            return;
        }
        PostParseContext context = new PostParseContext(lookup, parseContext);
        context.executePostParse();
    }

    private PostParseContext(MappingLookup mappingLookup, ParseContext pc) {
        this.searchLookup = new SearchLookup(
            mappingLookup::getFieldType,
            (ft, s) -> ft.fielddataBuilder(pc.indexSettings().getIndex().getName(), s).build(
                new IndexFieldDataCache.None(),
                new NoneCircuitBreakerService())
        );
        this.pc = pc;
        LazyDocumentReader reader = new LazyDocumentReader(
            pc.rootDoc(),
            pc.sourceToParse().source(),
            mappingLookup.getPostParsePhases().keySet());
        this.leafReaderContext = reader.getContext();
        mappingLookup.getPostParsePhases().forEach((k, c) -> fieldExecutors.put(k, new FieldExecutor(c)));
    }

    private void executePostParse() throws IOException {
        for (FieldExecutor ms : fieldExecutors.values()) {
            ms.execute();
        }
    }

    private void executeField(String field) throws IOException {
        assert fieldExecutors.containsKey(field);
        fieldExecutors.get(field).execute();
    }

    private class FieldExecutor {
        PostParseExecutor executor;
        boolean executed = false;
        FieldExecutor(PostParseExecutor executor) {
            this.executor = executor;
        }

        void execute() throws IOException {
            if (executed == false) {
                executor.execute(PostParseContext.this);
                executed = true;
            }
        }
    }

    private class LazyDocumentReader extends LeafReader {

        private final ParseContext.Document document;
        private final BytesReference sourceBytes;
        private final Set<String> calculatedFields;
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
                // memory index
                executeField(field);
                calculatedFields.remove(field);
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
