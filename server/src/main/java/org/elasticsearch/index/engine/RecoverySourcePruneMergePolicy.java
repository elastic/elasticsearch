/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.search.internal.FilterStoredFieldVisitor;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

final class RecoverySourcePruneMergePolicy extends OneMergeWrappingMergePolicy {
    RecoverySourcePruneMergePolicy(
        @Nullable String pruneStoredFieldName,
        String pruneNumericDVFieldName,
        boolean pruneIdField,
        boolean pruneSeqNo,
        Supplier<Query> retainSourceQuerySupplier,
        MergePolicy in,
        boolean useSyntheticId
    ) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(
                    pruneStoredFieldName,
                    pruneNumericDVFieldName,
                    pruneIdField,
                    pruneSeqNo,
                    wrapped,
                    retainSourceQuerySupplier,
                    useSyntheticId
                );
            }
        });
    }

    private static CodecReader wrapReader(
        String pruneStoredFieldName,
        String pruneNumericDVFieldName,
        boolean pruneIdField,
        boolean pruneSeqNo,
        CodecReader reader,
        Supplier<Query> retainSourceQuerySupplier,
        boolean useSyntheticId
    ) throws IOException {
        assert pruneSeqNo == false || reader.getPointValues(SeqNoFieldMapper.NAME) == null
            : "_seq_no points must not exist when sequence number pruning is enabled";
        NumericDocValues recoverySource = reader.getNumericDocValues(pruneNumericDVFieldName);
        final boolean hasRecoverySource = recoverySource != null && recoverySource.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
        NumericDocValues seqNoDocValues = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
        final boolean hasSeqNo = pruneSeqNo && seqNoDocValues != null && seqNoDocValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
        if (hasRecoverySource == false && hasSeqNo == false) {
            if (useSyntheticId) {
                return unwrapSyntheticIdStoredFieldsReader(reader);
            }
            return reader;  // early terminate - nothing to do here
        }
        IndexSearcher s = new IndexSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(retainSourceQuerySupplier.get()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer scorer = weight.scorer(reader.getContext());
        if (scorer != null) {
            BitSet recoverySourceToKeep = BitSet.of(scorer.iterator(), reader.maxDoc());
            // calculating the cardinality is significantly cheaper than skipping all bulk-merging we might do
            // if retentions are high we keep most of it
            if (recoverySourceToKeep.cardinality() == reader.maxDoc()) {
                if (useSyntheticId) {
                    return unwrapSyntheticIdStoredFieldsReader(reader);
                }
                return reader; // keep all source
            }
            return new PruningFilterCodecReader(
                pruneStoredFieldName,
                pruneNumericDVFieldName,
                pruneIdField,
                pruneSeqNo,
                reader,
                recoverySourceToKeep,
                useSyntheticId
            );
        } else {
            return new PruningFilterCodecReader(
                pruneStoredFieldName,
                pruneNumericDVFieldName,
                pruneIdField,
                pruneSeqNo,
                reader,
                null,
                useSyntheticId
            );
        }
    }

    private static class PruningFilterCodecReader extends FilterCodecReader {
        private final BitSet recoverySourceToKeep;
        private final String pruneStoredFieldName;
        private final String pruneNumericDVFieldName;
        private final boolean pruneIdField;
        private final boolean pruneSeqNo;
        private final boolean useSyntheticId;

        PruningFilterCodecReader(
            @Nullable String pruneStoredFieldName,
            String pruneNumericDVFieldName,
            boolean pruneIdField,
            boolean pruneSeqNo,
            CodecReader reader,
            BitSet recoverySourceToKeep,
            boolean useSyntheticId
        ) {
            super(reader);
            this.pruneStoredFieldName = pruneStoredFieldName;
            this.recoverySourceToKeep = recoverySourceToKeep;
            this.pruneNumericDVFieldName = pruneNumericDVFieldName;
            this.pruneIdField = pruneIdField;
            this.pruneSeqNo = pruneSeqNo;
            this.useSyntheticId = useSyntheticId;
        }

        private boolean shouldPruneNumericDocValues(String fieldName) {
            if (fieldName.equals(pruneNumericDVFieldName)) {
                return true;
            }
            return pruneSeqNo && fieldName.equals(SeqNoFieldMapper.NAME);
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer docValuesReader = super.getDocValuesReader();
            return new FilterDocValuesProducer(docValuesReader) {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                    NumericDocValues numeric = super.getNumeric(field);
                    if (shouldPruneNumericDocValues(field.name)) {
                        assert numeric != null : field.name + " must have numeric doc values but was null";
                        final DocIdSetIterator intersection;
                        if (recoverySourceToKeep == null) {
                            // we can't return null here Lucene's DocIdMerger expects an instance
                            intersection = DocIdSetIterator.empty();
                        } else {
                            intersection = ConjunctionUtils.intersectIterators(
                                Arrays.asList(numeric, new BitSetIterator(recoverySourceToKeep, recoverySourceToKeep.length()))
                            );
                        }
                        return new FilterNumericDocValues(numeric) {
                            @Override
                            public int nextDoc() throws IOException {
                                return intersection.nextDoc();
                            }

                            @Override
                            public int advance(int target) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public boolean advanceExact(int target) {
                                throw new UnsupportedOperationException();
                            }
                        };

                    }
                    return numeric;
                }
            };
        }

        @Override
        public StoredFieldsReader getFieldsReader() {
            StoredFieldsReader fieldsReader = super.getFieldsReader();
            if (useSyntheticId && fieldsReader instanceof TSDBStoredFieldsFormat.TSDBStoredFieldsReader tsdbReader) {
                fieldsReader = tsdbReader.getStoredFieldsReader();
            }
            if (pruneStoredFieldName == null && pruneIdField == false && useSyntheticId == false) {
                return fieldsReader;
            }
            return new PruningStoredFieldsReader(fieldsReader, recoverySourceToKeep, pruneStoredFieldName, pruneIdField, useSyntheticId);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }

        private static class PruningStoredFieldsReader extends FilterStoredFieldsReader {

            private final BitSet recoverySourceToKeep;
            private final String recoverySourceField;
            private final boolean pruneIdField;
            private final boolean useSyntheticId;

            PruningStoredFieldsReader(
                StoredFieldsReader in,
                BitSet recoverySourceToKeep,
                @Nullable String recoverySourceField,
                boolean pruneIdField,
                boolean useSyntheticId
            ) {
                super(in);
                assert recoverySourceField != null || pruneIdField || useSyntheticId : "nothing to prune";
                this.recoverySourceToKeep = recoverySourceToKeep;
                this.recoverySourceField = recoverySourceField;
                this.pruneIdField = pruneIdField;
                this.useSyntheticId = useSyntheticId;
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                if (recoverySourceToKeep != null && recoverySourceToKeep.get(docID)) {
                    if (useSyntheticId) {
                        super.document(docID, new SkipIdFieldVisitor(visitor));
                    } else {
                        super.document(docID, visitor);
                    }
                } else {
                    super.document(docID, new FilterStoredFieldVisitor(visitor) {
                        @Override
                        public Status needsField(FieldInfo fieldInfo) throws IOException {
                            if (fieldInfo.name.equals(recoverySourceField)) {
                                return Status.NO;
                            }
                            if ((pruneIdField || useSyntheticId) && IdFieldMapper.NAME.equals(fieldInfo.name)) {
                                return Status.NO;
                            }
                            return super.needsField(fieldInfo);
                        }
                    });
                }
            }

            @Override
            public StoredFieldsReader getMergeInstance() {
                return new PruningStoredFieldsReader(
                    in.getMergeInstance(),
                    recoverySourceToKeep,
                    recoverySourceField,
                    pruneIdField,
                    useSyntheticId
                );
            }

            @Override
            public StoredFieldsReader clone() {
                return new PruningStoredFieldsReader(in.clone(), recoverySourceToKeep, recoverySourceField, pruneIdField, useSyntheticId);
            }
        }
    }

    private abstract static class FilterStoredFieldsReader extends StoredFieldsReader {

        protected final StoredFieldsReader in;

        FilterStoredFieldsReader(StoredFieldsReader fieldsReader) {
            this.in = fieldsReader;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            in.document(docID, visitor);
        }

        @Override
        public abstract StoredFieldsReader clone();

        @Override
        public void checkIntegrity() throws IOException {
            in.checkIntegrity();
        }
    }

    /**
     * When synthetic _id is used, the codec wraps the stored fields reader with a {@link TSDBStoredFieldsFormat.TSDBStoredFieldsReader}
     * that materializes synthetic _id values from doc values. During merges, synthetic _id values should not be materialized, so we
     * unwrap the stored fields reader to return the inner (non-synthetic) reader. This also allows Lucene to use optimized bulk-merges.
     * If the stored fields reader cannot be unwrapped (e.g. when wrapped by an intermediate layer), we fall back to a reader that
     * skips the _id field via a {@link FilterStoredFieldVisitor} to prevent materialization on merge threads.
     */
    private static CodecReader unwrapSyntheticIdStoredFieldsReader(CodecReader reader) {
        return new FilterCodecReader(reader) {
            @Override
            public StoredFieldsReader getFieldsReader() {
                StoredFieldsReader fieldsReader = super.getFieldsReader();
                if (fieldsReader instanceof TSDBStoredFieldsFormat.TSDBStoredFieldsReader tsdbReader) {
                    return tsdbReader.getStoredFieldsReader();
                }
                // The TSDBStoredFieldsReader is hidden behind an intermediate wrapper: bulk merging is already impossible in this case so
                // we fall back to skipping _id via a visitor filter.
                return new SkipSyntheticIdFilterStoredFieldsReader(fieldsReader);
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    /**
     * A {@link StoredFieldsReader} that filters out the synthetic {@code _id} field during document reads.
     *
     * Used as a fallback when the {@link TSDBStoredFieldsFormat.TSDBStoredFieldsReader} cannot be unwrapped
     * (e.g. when an intermediate wrapper like {@code SlowCodecReaderWrapper} hides it). In such cases bulk merging
     * is already impossible, so this wrapper ensures that the synthetic {@code _id} is not materialized on merge threads.
     */
    private static class SkipSyntheticIdFilterStoredFieldsReader extends FilterStoredFieldsReader {

        SkipSyntheticIdFilterStoredFieldsReader(StoredFieldsReader in) {
            super(in);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            super.document(docID, new SkipIdFieldVisitor(visitor));
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new SkipSyntheticIdFilterStoredFieldsReader(in.getMergeInstance());
        }

        @Override
        public StoredFieldsReader clone() {
            return new SkipSyntheticIdFilterStoredFieldsReader(in.clone());
        }
    }

    /**
     * A {@link FilterStoredFieldVisitor} that skips the synthetic {@code _id} field, preventing it from being
     * visited during merges.
     */
    private static class SkipIdFieldVisitor extends FilterStoredFieldVisitor {

        SkipIdFieldVisitor(StoredFieldVisitor visitor) {
            super(visitor);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.NO;
            }
            return super.needsField(fieldInfo);
        }
    }
}
