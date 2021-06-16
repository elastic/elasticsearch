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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

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

    public enum UsageContext {
        DOC_VALUES,
        STORED_FIELDS,
        TERMS,
        FREQS,
        POSITIONS,
        OFFSETS,
        NORMS,
        PAYLOADS,
        IMPACTS,
        TERM_VECTORS, // possibly refine this one
        POINTS,
    }

    public interface FieldUsageNotifier {
        void onFieldUsage(String field, UsageContext usageContext);
    }

    public static final class FieldUsageTrackingLeafReader extends SequentialStoredFieldsLeafReader {

        private final FieldUsageNotifier notifier;

        public FieldUsageTrackingLeafReader(LeafReader in, FieldUsageNotifier notifier) {
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
        public PointValues getPointValues(String field) throws IOException {
            PointValues pointValues = super.getPointValues(field);
            if (pointValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.POINTS);
            }
            return pointValues;
        }

        @Override
        public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
            super.document(docID, new FilterStoredFieldVisitor(visitor) {
                @Override
                public Status needsField(FieldInfo fieldInfo) throws IOException {
                    Status status = visitor.needsField(fieldInfo);
                    if (status == Status.YES) {
                        notifier.onFieldUsage(fieldInfo.name, FieldUsageTrackingDirectoryReader.UsageContext.STORED_FIELDS);
                    }
                    return status;
                }
            });
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.TERMS);
                terms = new FieldUsageTrackingTerms(field, terms);
            }
            return terms;
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            BinaryDocValues binaryDocValues = super.getBinaryDocValues(field);
            if (binaryDocValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.DOC_VALUES);
            }
            return binaryDocValues;
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            SortedDocValues sortedDocValues = super.getSortedDocValues(field);
            if (sortedDocValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.DOC_VALUES);
            }
            return sortedDocValues;
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            SortedNumericDocValues sortedNumericDocValues = super.getSortedNumericDocValues(field);
            if (sortedNumericDocValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.DOC_VALUES);
            }
            return sortedNumericDocValues;
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            SortedSetDocValues sortedSetDocValues = super.getSortedSetDocValues(field);
            if (sortedSetDocValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.DOC_VALUES);
            }
            return sortedSetDocValues;
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            NumericDocValues numericDocValues = super.getNormValues(field);
            if (numericDocValues != null) {
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.NORMS);
            }
            return numericDocValues;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FieldUsageTrackingLeafReader(reader=");
            return sb.append(in).append(')').toString();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        private class FieldUsageTrackingTerms extends FilterTerms {

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
            public long getSumTotalTermFreq() throws IOException {
                long totalTermFreq = super.getSumTotalTermFreq();
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.FREQS);
                return totalTermFreq;
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return in.getSumDocFreq();
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
                notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.FREQS);
                return totalTermFreq;
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                PostingsEnum postingsEnum = super.postings(reuse, flags);
                if (postingsEnum != null) {
                    checkPostingsFlags(flags);
                }
                return postingsEnum;
            }

            @Override
            public ImpactsEnum impacts(int flags) throws IOException {
                ImpactsEnum impactsEnum = super.impacts(flags);
                if (impactsEnum != null) {
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.IMPACTS);
                    checkPostingsFlags(flags);
                }
                return impactsEnum;
            }

            private void checkPostingsFlags(int flags) {
                if (PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.FREQS);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.POSITIONS);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.OFFSETS);
                }
                if (PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS)) {
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.PAYLOADS);
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
                    notifier.onFieldUsage(field, FieldUsageTrackingDirectoryReader.UsageContext.TERM_VECTORS);
                }
                return terms;
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
