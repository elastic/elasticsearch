/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.fieldvisitor.FieldNamesProvidingStoredFieldsVisitor;

import java.io.IOException;

/**
 * Wraps a DirectoryReader and tracks all access to fields, notifying a
 * {@link FieldUsageNotifier} upon access.
 */
public class FieldUsageTrackingDirectoryReader extends FilterDirectoryReader {

    private final FieldUsageNotifier notifier;

    public FieldUsageTrackingDirectoryReader(DirectoryReader in, FieldUsageNotifier notifier) throws IOException {
        super(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new FieldUsageTrackingLeafReader(reader, notifier);
            }
        });
        this.notifier = notifier;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new FieldUsageTrackingDirectoryReader(in, notifier);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public interface FieldUsageNotifier {
        void onTermsUsed(String field);

        void onPostingsUsed(String field);

        void onTermFrequenciesUsed(String field);

        void onPositionsUsed(String field);

        void onOffsetsUsed(String field);

        void onDocValuesUsed(String field);

        void onStoredFieldsUsed(String field);

        void onNormsUsed(String field);

        void onPayloadsUsed(String field);

        void onPointsUsed(String field);

        void onTermVectorsUsed(String field);

        void onKnnVectorsUsed(String field);
    }

    static final class FieldUsageTrackingLeafReader extends SequentialStoredFieldsLeafReader {

        private final FieldUsageNotifier notifier;

        FieldUsageTrackingLeafReader(LeafReader in, FieldUsageNotifier notifier) {
            super(in);
            this.notifier = notifier;
        }

        @Override
        public Fields getTermVectors(int docID) throws IOException {
            Fields f = super.getTermVectors(docID);
            if (f != null) {
                f = new FieldUsageTrackingTermVectorFields(f);
            }
            return f;
        }

        @Override
        public TermVectors termVectors() throws IOException {
            TermVectors termVectors = super.termVectors();
            return new TermVectors() {
                @Override
                public Fields get(int doc) throws IOException {
                    Fields f = termVectors.get(doc);
                    if (f != null) {
                        f = new FieldUsageTrackingTermVectorFields(f);
                    }
                    return f;
                }
            };
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            PointValues pointValues = super.getPointValues(field);
            if (pointValues != null) {
                notifier.onPointsUsed(field);
            }
            return pointValues;
        }

        @Override
        public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
            storedFields().document(docID, visitor);
        }

        @Override
        public StoredFields storedFields() throws IOException {
            StoredFields storedFields = super.storedFields();
            return new StoredFields() {
                @Override
                public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                    if (visitor instanceof FieldNamesProvidingStoredFieldsVisitor) {
                        storedFields.document(docID, new FieldUsageFieldsVisitor((FieldNamesProvidingStoredFieldsVisitor) visitor));
                    } else {
                        storedFields.document(docID, new FieldUsageStoredFieldVisitor(visitor));
                    }
                }
            };
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms != null) {
                notifier.onTermsUsed(field);
                // we can't wrap CompletionTerms, as CompletionWeight does an instanceof check.
                // we also can't create a subclass of CompletionTerms as it is a final class.
                // TODO: fix in Lucene
                if (terms instanceof CompletionTerms == false) {
                    terms = new FieldUsageTrackingTerms(field, terms);
                }
            }
            return terms;
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            BinaryDocValues binaryDocValues = super.getBinaryDocValues(field);
            if (binaryDocValues != null) {
                notifier.onDocValuesUsed(field);
            }
            return binaryDocValues;
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            SortedDocValues sortedDocValues = super.getSortedDocValues(field);
            if (sortedDocValues != null) {
                notifier.onDocValuesUsed(field);
            }
            return sortedDocValues;
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            SortedNumericDocValues sortedNumericDocValues = super.getSortedNumericDocValues(field);
            if (sortedNumericDocValues != null) {
                notifier.onDocValuesUsed(field);
            }
            return sortedNumericDocValues;
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            SortedSetDocValues sortedSetDocValues = super.getSortedSetDocValues(field);
            if (sortedSetDocValues != null) {
                notifier.onDocValuesUsed(field);
            }
            return sortedSetDocValues;
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            NumericDocValues numericDocValues = super.getNormValues(field);
            if (numericDocValues != null) {
                notifier.onNormsUsed(field);
            }
            return numericDocValues;
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            FloatVectorValues vectorValues = super.getFloatVectorValues(field);
            if (vectorValues != null) {
                notifier.onKnnVectorsUsed(field);
            }
            return vectorValues;
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            ByteVectorValues vectorValues = super.getByteVectorValues(field);
            if (vectorValues != null) {
                notifier.onKnnVectorsUsed(field);
            }
            return vectorValues;
        }

        @Override
        public TopDocs searchNearestVectors(String field, byte[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
            TopDocs topDocs = super.searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
            if (topDocs != null) {
                notifier.onKnnVectorsUsed(field);
            }
            return topDocs;
        }

        @Override
        public TopDocs searchNearestVectors(String field, float[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
            TopDocs topDocs = super.searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
            if (topDocs != null) {
                notifier.onKnnVectorsUsed(field);
            }
            return topDocs;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FieldUsageTrackingLeafReader(reader=");
            return sb.append(in).append(')').toString();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return new FieldUsageTrackingStoredFieldsReader(reader);
        }

        class FieldUsageTrackingStoredFieldsReader extends StoredFieldsReader {
            final StoredFieldsReader reader;

            FieldUsageTrackingStoredFieldsReader(StoredFieldsReader reader) {
                this.reader = reader;
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                reader.document(docID, new FieldUsageStoredFieldVisitor(visitor));
            }

            @Override
            public StoredFieldsReader clone() {
                return new FieldUsageTrackingStoredFieldsReader(reader.clone());
            }

            @Override
            public StoredFieldsReader getMergeInstance() {
                return new FieldUsageTrackingStoredFieldsReader(reader.getMergeInstance());
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

        class FieldUsageTrackingTerms extends FilterTerms {

            private final String field;

            FieldUsageTrackingTerms(String field, Terms in) {
                super(in);
                this.field = field;
            }

            @Override
            public TermsEnum iterator() throws IOException {
                TermsEnum termsEnum = in.iterator();
                if (termsEnum != null) {
                    termsEnum = new FieldUsageTrackingTermsEnum(field, termsEnum);
                }
                return termsEnum;
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
                TermsEnum termsEnum = in.intersect(compiled, startTerm);
                if (termsEnum != null) {
                    termsEnum = new FieldUsageTrackingTermsEnum(field, termsEnum);
                }
                return termsEnum;
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                long totalTermFreq = super.getSumTotalTermFreq();
                notifier.onTermFrequenciesUsed(field);
                return totalTermFreq;
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return in.getSumDocFreq();
            }

            @Override
            public BytesRef getMin() throws IOException {
                return in.getMin();
            }

            @Override
            public BytesRef getMax() throws IOException {
                return in.getMax();
            }
        }

        private class FieldUsageTrackingTermsEnum extends FilterTermsEnum {

            private final String field;

            FieldUsageTrackingTermsEnum(String field, TermsEnum in) {
                super(in);
                this.field = field;
            }

            @Override
            public long totalTermFreq() throws IOException {
                long totalTermFreq = super.totalTermFreq();
                notifier.onTermFrequenciesUsed(field);
                return totalTermFreq;
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                PostingsEnum postingsEnum = super.postings(reuse, flags);
                if (postingsEnum != null) {
                    notifier.onPostingsUsed(field);
                    checkPostingsFlags(flags);
                }
                return postingsEnum;
            }

            @Override
            public ImpactsEnum impacts(int flags) throws IOException {
                ImpactsEnum impactsEnum = super.impacts(flags);
                if (impactsEnum != null) {
                    notifier.onPostingsUsed(field);
                    checkPostingsFlags(flags);
                }
                return impactsEnum;
            }

            private void checkPostingsFlags(int flags) {
                if (PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
                    notifier.onTermFrequenciesUsed(field);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
                    notifier.onPositionsUsed(field);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
                    notifier.onOffsetsUsed(field);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS)) {
                    notifier.onPayloadsUsed(field);
                }
            }

        }

        private class FieldUsageTrackingTermVectorFields extends FilterFields {

            FieldUsageTrackingTermVectorFields(Fields in) {
                super(in);
            }

            @Override
            public Terms terms(String field) throws IOException {
                Terms terms = super.terms(field);
                if (terms != null) {
                    notifier.onTermVectorsUsed(field);
                }
                return terms;
            }

        }

        private class FieldUsageStoredFieldVisitor extends FilterStoredFieldVisitor {
            FieldUsageStoredFieldVisitor(StoredFieldVisitor visitor) {
                super(visitor);
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                Status status = super.needsField(fieldInfo);
                if (status == Status.YES) {
                    notifier.onStoredFieldsUsed(fieldInfo.name);
                }
                return status;
            }
        }

        private class FieldUsageFieldsVisitor extends FilterFieldNamesProvidingStoredFieldsVisitor {
            FieldUsageFieldsVisitor(FieldNamesProvidingStoredFieldsVisitor visitor) {
                super(visitor);
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                Status status = super.needsField(fieldInfo);
                if (status == Status.YES) {
                    notifier.onStoredFieldsUsed(fieldInfo.name);
                }
                return status;
            }
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
