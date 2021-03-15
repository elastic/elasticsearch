/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
     */
    public static void executePostParsePhases(MappingLookup lookup, ParseContext parseContext) {
        if (lookup.getPostParseExecutors().isEmpty()) {
            return;
        }
        PostParsePhase postParsePhase = new PostParsePhase(lookup, parseContext);
        for (OneTimeFieldExecutor executor : postParsePhase.fieldExecutors.values()) {
            executor.execute();
        }
    }

    private PostParsePhase(MappingLookup mappingLookup, ParseContext pc) {
        LazyDocumentReader reader = new LazyDocumentReader(
            pc.rootDoc(),
            pc.sourceToParse().source(),
            mappingLookup.getPostParseExecutors().keySet());
        this.context = new PostParseContext(mappingLookup, pc, reader.getContext());
        mappingLookup.getPostParseExecutors().forEach((k, c) -> fieldExecutors.put(k, new OneTimeFieldExecutor(c)));
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

        void execute() {
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

        private LazyDocumentReader(ParseContext.Document document, BytesReference sourceBytes, Set<String> calculatedFields) {
            this.document = document;
            this.sourceBytes = sourceBytes;
            this.calculatedFields = calculatedFields;
        }

        private void checkField(String field) {
            if (calculatedFields.contains(field)) {
                // this means that a mapper script is referring to another calculated field;
                // in which case we need to execute that field first. We also check for loops here
                if (fieldPath.add(field) == false) {
                    throw new IllegalStateException("Loop in field resolution detected: " + String.join("->", fieldPath) + "->" + field);
                }
                assert fieldExecutors.containsKey(field);
                fieldExecutors.get(field).execute();
                calculatedFields.remove(field);
                fieldPath.remove(field);
            }
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            checkField(field);
            List<Number> values = document.getFields().stream()
                .filter(f -> Objects.equals(f.name(), field))
                .filter(f -> f.fieldType().docValuesType() == DocValuesType.NUMERIC)
                .map(IndexableField::numericValue)
                .sorted()
                .collect(Collectors.toList());
            return numericDocValues(values);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            checkField(field);
            List<BytesRef> values = document.getFields().stream()
                .filter(f -> Objects.equals(f.name(), field))
                .filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY)
                .map(IndexableField::binaryValue)
                .sorted()
                .collect(Collectors.toList());
            return binaryDocValues(values);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            checkField(field);
            List<BytesRef> values = document.getFields().stream()
                .filter(f -> Objects.equals(f.name(), field))
                .filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY)
                .map(IndexableField::binaryValue)
                .sorted()
                .collect(Collectors.toList());
            return sortedDocValues(values);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            checkField(field);
            List<Number> values = document.getFields().stream()
                .filter(f -> Objects.equals(f.name(), field))
                .filter(f -> f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
                .map(IndexableField::numericValue)
                .sorted()
                .collect(Collectors.toList());
            return sortedNumericDocValues(values);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            List<BytesRef> values = document.getFields().stream()
                .filter(f -> Objects.equals(f.name(), field))
                .filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY)
                .map(IndexableField::binaryValue)
                .sorted()
                .collect(Collectors.toList());
            return sortedSetDocValues(values);
        }

        @Override
        public FieldInfos getFieldInfos() {
            throw new UnsupportedOperationException();
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

    private static NumericDocValues numericDocValues(List<Number> values) {
        if (values.size() == 0) {
            return null;
        }
        return new NumericDocValues() {
            @Override
            public long longValue() {
                return values.get(0).longValue();
            }

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    private static SortedNumericDocValues sortedNumericDocValues(List<Number> values) {
        if (values.size() == 0) {
            return null;
        }
        return new SortedNumericDocValues() {

            int i = -1;

            @Override
            public long nextValue() {
                i++;
                return values.get(i).longValue();
            }

            @Override
            public int docValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    private static BinaryDocValues binaryDocValues(List<BytesRef> values) {
        if (values.size() == 0) {
            return null;
        }
        return new BinaryDocValues() {
            @Override
            public BytesRef binaryValue() {
                return values.get(0);
            }

            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    private static SortedDocValues sortedDocValues(List<BytesRef> values) {
        if (values.size() == 0) {
            return null;
        }
        return new SortedDocValues() {

            @Override
            public int ordValue() {
                return 0;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return values.get(0);
            }

            @Override
            public int getValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    private static SortedSetDocValues sortedSetDocValues(List<BytesRef> values) {
        if (values.size() == 0) {
            return null;
        }
        return new SortedSetDocValues() {

            int i = -1;

            @Override
            public long nextOrd() {
                i++;
                return i;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values.get((int)ord);
            }

            @Override
            public long getValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }
}
