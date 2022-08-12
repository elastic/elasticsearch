/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FilterIterator;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link FilterLeafReader} that exposes only a subset
 * of fields from the underlying wrapped reader.
 */
// based on lucene/test-framework's FieldFilterLeafReader.
public final class FieldSubsetReader extends SequentialStoredFieldsLeafReader {

    /**
     * Wraps a provided DirectoryReader, exposing a subset of fields.
     * <p>
     * Note that for convenience, the returned reader
     * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
     * and so on.
     * @param in reader to filter
     * @param filter fields to filter.
     */
    public static DirectoryReader wrap(DirectoryReader in, CharacterRunAutomaton filter) throws IOException {
        return new FieldSubsetDirectoryReader(in, filter);
    }

    // wraps subreaders with fieldsubsetreaders.
    static class FieldSubsetDirectoryReader extends FilterDirectoryReader {

        private final CharacterRunAutomaton filter;

        FieldSubsetDirectoryReader(DirectoryReader in, final CharacterRunAutomaton filter) throws IOException {
            super(in, new FilterDirectoryReader.SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new FieldSubsetReader(reader, filter);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
            this.filter = filter;
            verifyNoOtherFieldSubsetDirectoryReaderIsWrapped(in);
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new FieldSubsetDirectoryReader(in, filter);
        }

        /** Return the automaton that is used to filter fields. */
        CharacterRunAutomaton getFilter() {
            return filter;
        }

        private static void verifyNoOtherFieldSubsetDirectoryReaderIsWrapped(DirectoryReader reader) {
            if (reader instanceof FilterDirectoryReader filterDirectoryReader) {
                if (filterDirectoryReader instanceof FieldSubsetDirectoryReader) {
                    throw new IllegalArgumentException(
                        LoggerMessageFormat.format("Can't wrap [{}] twice", FieldSubsetDirectoryReader.class)
                    );
                } else {
                    verifyNoOtherFieldSubsetDirectoryReaderIsWrapped(filterDirectoryReader.getDelegate());
                }
            }
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /** List of filtered fields */
    private final FieldInfos fieldInfos;
    /** An automaton that only accepts authorized fields. */
    private final CharacterRunAutomaton filter;
    /** {@link Terms} cache with filtered stats for the {@link FieldNamesFieldMapper} field. */
    private volatile Optional<Terms> fieldNamesFilterTerms;

    /**
     * Wrap a single segment, exposing a subset of its fields.
     */
    FieldSubsetReader(LeafReader in, CharacterRunAutomaton filter) throws IOException {
        super(in);
        ArrayList<FieldInfo> filteredInfos = new ArrayList<>();
        for (FieldInfo fi : in.getFieldInfos()) {
            if (filter.run(fi.name)) {
                filteredInfos.add(fi);
            }
        }
        fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
        this.filter = filter;
    }

    /** returns true if this field is allowed. */
    boolean hasField(String field) {
        return fieldInfos.fieldInfo(field) != null;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
        Fields f = super.getTermVectors(docID);
        if (f == null) {
            return null;
        }
        f = new FieldFilterFields(f);
        // we need to check for emptyness, so we can return null:
        return f.iterator().hasNext() ? f : null;
    }

    /** Filter a map by a {@link CharacterRunAutomaton} that defines the fields to retain. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> filter(Map<String, ?> map, CharacterRunAutomaton includeAutomaton, int initialState) {
        Map<String, Object> filtered = new HashMap<>();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String key = entry.getKey();

            int state = step(includeAutomaton, key, initialState);
            if (state == -1) {
                continue;
            }

            Object value = entry.getValue();

            if (value instanceof Map) {
                state = includeAutomaton.step(state, '.');
                if (state == -1) {
                    continue;
                }

                Map<String, ?> mapValue = (Map<String, ?>) value;
                Map<String, Object> filteredValue = filter(mapValue, includeAutomaton, state);
                if (filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }
            } else if (value instanceof Iterable<?> iterableValue) {
                List<Object> filteredValue = filter(iterableValue, includeAutomaton, state);
                if (filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }
            } else if (includeAutomaton.isAccept(state)) {
                filtered.put(key, value);
            }
        }
        return filtered;
    }

    /** Filter a list by a {@link CharacterRunAutomaton} that defines the fields to retain. */
    @SuppressWarnings("unchecked")
    private static List<Object> filter(Iterable<?> iterable, CharacterRunAutomaton includeAutomaton, int initialState) {
        List<Object> filtered = new ArrayList<>();
        for (Object value : iterable) {
            if (value instanceof Map) {
                int state = includeAutomaton.step(initialState, '.');
                if (state == -1) {
                    continue;
                }
                Map<String, Object> filteredValue = filter((Map<String, ?>) value, includeAutomaton, state);
                filtered.add(filteredValue);
            } else if (value instanceof Iterable) {
                List<Object> filteredValue = filter((Iterable<?>) value, includeAutomaton, initialState);
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (includeAutomaton.isAccept(initialState)) {
                filtered.add(value);
            }
        }
        return filtered;
    }

    /** Step through all characters of the provided string, and return the
     *  resulting state, or -1 if that did not lead to a valid state. */
    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

    @Override
    public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
        super.document(docID, new FieldSubsetStoredFieldVisitor(visitor));
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return new FieldSubsetStoredFieldsReader(reader);
    }

    @Override
    public Terms terms(String field) throws IOException {
        return wrapTerms(super.terms(field), field);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return hasField(field) ? super.getNumericDocValues(field) : null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return hasField(field) ? super.getBinaryDocValues(field) : null;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedDocValues(field) : null;
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedNumericDocValues(field) : null;
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedSetDocValues(field) : null;
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return hasField(field) ? super.getNormValues(field) : null;
    }

    @Override
    public VectorValues getVectorValues(String field) throws IOException {
        return hasField(field) ? super.getVectorValues(field) : null;
    }

    @Override
    public TopDocs searchNearestVectors(String field, float[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
        return hasField(field) ? super.searchNearestVectors(field, target, k, acceptDocs, visitedLimit) : null;
    }

    // we share core cache keys (for e.g. fielddata)

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /**
     * StoredFields impl that filters out stored fields and source fields that should not be visible.
     */
    class FieldSubsetStoredFieldsReader extends StoredFieldsReader {
        final StoredFieldsReader reader;

        FieldSubsetStoredFieldsReader(StoredFieldsReader reader) {
            this.reader = reader;
        }

        @Override
        public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
            reader.visitDocument(docID, new FieldSubsetStoredFieldVisitor(visitor));
        }

        @Override
        public StoredFieldsReader clone() {
            return new FieldSubsetStoredFieldsReader(reader.clone());
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new FieldSubsetStoredFieldsReader(reader.getMergeInstance());
        }

        @Override
        public void checkIntegrity() throws IOException {
            reader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /**
     * A field visitor that filters out stored fields and source fields that should not be visible.
     */
    class FieldSubsetStoredFieldVisitor extends StoredFieldVisitor {
        final StoredFieldVisitor visitor;

        FieldSubsetStoredFieldVisitor(StoredFieldVisitor visitor) {
            this.visitor = visitor;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                // for _source, parse, filter out the fields we care about, and serialize back downstream
                BytesReference bytes = new BytesArray(value);
                Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(bytes, true);
                Map<String, Object> transformedSource = filter(result.v2(), filter, 0);
                XContentBuilder xContentBuilder = XContentBuilder.builder(result.v1().xContent()).map(transformedSource);
                visitor.binaryField(fieldInfo, BytesReference.toBytes(BytesReference.bytes(xContentBuilder)));
            } else {
                visitor.binaryField(fieldInfo, value);
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) throws IOException {
            visitor.stringField(fieldInfo, value);
        }

        @Override
        public void intField(FieldInfo fieldInfo, int value) throws IOException {
            visitor.intField(fieldInfo, value);
        }

        @Override
        public void longField(FieldInfo fieldInfo, long value) throws IOException {
            visitor.longField(fieldInfo, value);
        }

        @Override
        public void floatField(FieldInfo fieldInfo, float value) throws IOException {
            visitor.floatField(fieldInfo, value);
        }

        @Override
        public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
            visitor.doubleField(fieldInfo, value);
        }

        @Override
        public StoredFieldVisitor.Status needsField(FieldInfo fieldInfo) throws IOException {
            return hasField(fieldInfo.name) ? visitor.needsField(fieldInfo) : StoredFieldVisitor.Status.NO;
        }
    }

    /**
     * Filters the Fields instance from the postings.
     * <p>
     * In addition to only returning fields allowed in this subset,
     * the ES internal _field_names (used by exists filter) has special handling,
     * to hide terms for fields that don't exist.
     */
    class FieldFilterFields extends FilterFields {

        FieldFilterFields(Fields in) {
            super(in);
        }

        @Override
        public int size() {
            // this information is not cheap, return -1 like MultiFields does:
            return -1;
        }

        @Override
        public Iterator<String> iterator() {
            return new FilterIterator<String, String>(super.iterator()) {
                @Override
                protected boolean predicateFunction(String field) {
                    return hasField(field);
                }
            };
        }

        @Override
        public Terms terms(String field) throws IOException {
            return wrapTerms(super.terms(field), field);
        }
    }

    private Terms wrapTerms(Terms terms, String field) throws IOException {
        if (hasField(field) == false) {
            return null;
        } else if (FieldNamesFieldMapper.NAME.equals(field)) {
            // For the _field_names field, fields for the document are encoded as postings, where term is the field, so we hide terms for
            // fields we filter out.
            // Compute this lazily so that the DirectoryReader wrapper works together with RewriteCachingDirectoryReader (used by the
            // can match phase in the frozen tier), which does not implement the terms() method.
            if (fieldNamesFilterTerms == null) {
                synchronized (this) {
                    if (fieldNamesFilterTerms == null) {
                        assert Transports.assertNotTransportThread("resolving filter terms");
                        final Terms fieldNameTerms = super.terms(FieldNamesFieldMapper.NAME);
                        this.fieldNamesFilterTerms = fieldNameTerms == null
                            ? Optional.empty()
                            : Optional.of(new FieldNamesTerms(fieldNameTerms));
                    }
                }
            }
            return fieldNamesFilterTerms.orElse(null);
        } else {
            return terms;
        }
    }

    /**
     * Terms impl for _field_names (used by exists filter) that filters out terms
     * representing fields that should not be visible in this reader.
     */
    class FieldNamesTerms extends FilterTerms {
        final long size;
        final long sumDocFreq;
        final long sumTotalFreq;

        FieldNamesTerms(Terms in) throws IOException {
            super(in);
            assert in.hasFreqs() == false;
            // re-compute the stats for the field to take
            // into account the filtered terms.
            final TermsEnum e = iterator();
            long size = 0, sumDocFreq = 0, sumTotalFreq = 0;
            while (e.next() != null) {
                size++;
                sumDocFreq += e.docFreq();
                sumTotalFreq += e.totalTermFreq();
            }
            this.size = size;
            this.sumDocFreq = sumDocFreq;
            this.sumTotalFreq = sumTotalFreq;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new FieldNamesTermsEnum(in.iterator());
        }

        @Override
        public long size() throws IOException {
            return size;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return sumDocFreq;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return sumTotalFreq;
        }

        @Override
        public int getDocCount() throws IOException {
            // it is costly to recompute this value so we assume that docCount == maxDoc.
            return maxDoc();
        }
    }

    /**
     * TermsEnum impl for _field_names (used by exists filter) that filters out terms
     * representing fields that should not be visible in this reader.
     */
    class FieldNamesTermsEnum extends FilterTermsEnum {

        FieldNamesTermsEnum(TermsEnum in) {
            super(in);
        }

        /** Return true if term is accepted (matches a field name in this reader). */
        boolean accept(BytesRef term) {
            return hasField(term.utf8ToString());
        }

        @Override
        public boolean seekExact(BytesRef term) throws IOException {
            return accept(term) && in.seekExact(term);
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            if (accept(term) == false) {
                throw new IllegalStateException("Tried to seek using a TermState from a different reader!");
            }
            in.seekExact(term, state);
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) throws IOException {
            SeekStatus status = in.seekCeil(term);
            if (status == SeekStatus.END || accept(term())) {
                return status;
            }
            return next() == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
        }

        @Override
        public BytesRef next() throws IOException {
            BytesRef next;
            while ((next = in.next()) != null) {
                if (accept(next)) {
                    break;
                }
            }
            return next;
        }

        // we don't support ordinals, but _field_names is not used in this way

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public PointValues getPointValues(String fieldName) throws IOException {
        if (hasField(fieldName)) {
            return super.getPointValues(fieldName);
        } else {
            return null;
        }
    }
}
