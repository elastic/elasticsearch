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
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;

public class IndexTimeScriptContext {

    public final SearchLookup searchLookup;
    public final LeafReaderContext leafReaderContext;

    public IndexTimeScriptContext(SearchLookup searchLookup, ParseContext.Document doc, BytesReference sourceBytes) {
        this.searchLookup = searchLookup;
        LazyDocumentReader reader = new LazyDocumentReader(doc, sourceBytes);
        this.leafReaderContext = reader.getContext();
    }

    private static class LazyDocumentReader extends LeafReader {

        private final ParseContext.Document document;
        private final BytesReference sourceBytes;
        private LeafReaderContext in;

        private LazyDocumentReader(ParseContext.Document document, BytesReference sourceBytes) {
            this.document = document;
            this.sourceBytes = sourceBytes;
        }

        private void buildDocValues() {
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
            buildDocValues();
            return in.reader().getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            buildDocValues();
            return in.reader().getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            buildDocValues();
            return in.reader().getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            buildDocValues();
            return in.reader().getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            buildDocValues();
            return in.reader().getSortedSetDocValues(field);
        }

        @Override
        public FieldInfos getFieldInfos() {
            buildDocValues();
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
