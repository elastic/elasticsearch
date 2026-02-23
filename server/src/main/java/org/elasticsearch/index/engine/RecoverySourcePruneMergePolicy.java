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
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.search.internal.FilterStoredFieldVisitor;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

final class RecoverySourcePruneMergePolicy extends OneMergeWrappingMergePolicy {
    RecoverySourcePruneMergePolicy(
        @Nullable String pruneStoredFieldName,
        String pruneNumericDVFieldName,
        boolean pruneIdField,
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
        CodecReader reader,
        Supplier<Query> retainSourceQuerySupplier,
        boolean useSyntheticId
    ) throws IOException {
        CodecReader finalReader;
        if (useSyntheticId) {
            // Wraps the codec reader to avoid reading synthetic id stored field values during merges. This is important to avoid synthetic
            // ids to be materialized from doc values for every document during merges and to avoid synthetic id to be stored back on disk
            // in merged segments.
            finalReader = new SkipSyntheticIdFilterCodecReader(reader);
        } else {
            finalReader = reader;
        }

        NumericDocValues recoverySource = finalReader.getNumericDocValues(pruneNumericDVFieldName);
        if (recoverySource == null || recoverySource.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return finalReader; // early terminate - nothing to do here since none of the docs has a recovery source anymore.
        }
        IndexSearcher s = new IndexSearcher(finalReader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(retainSourceQuerySupplier.get()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer scorer = weight.scorer(finalReader.getContext());
        if (scorer != null) {
            BitSet recoverySourceToKeep = BitSet.of(scorer.iterator(), finalReader.maxDoc());
            // calculating the cardinality is significantly cheaper than skipping all bulk-merging we might do
            // if retentions are high we keep most of it
            if (recoverySourceToKeep.cardinality() == finalReader.maxDoc()) {
                return finalReader; // keep all source
            }
            return new SourcePruningFilterCodecReader(
                pruneStoredFieldName,
                pruneNumericDVFieldName,
                pruneIdField,
                finalReader,
                recoverySourceToKeep
            );
        } else {
            return new SourcePruningFilterCodecReader(pruneStoredFieldName, pruneNumericDVFieldName, pruneIdField, finalReader, null);
        }
    }

    private static class SourcePruningFilterCodecReader extends FilterCodecReader {
        private final BitSet recoverySourceToKeep;
        private final String pruneStoredFieldName;
        private final String pruneNumericDVFieldName;
        private final boolean pruneIdField;

        SourcePruningFilterCodecReader(
            @Nullable String pruneStoredFieldName,
            String pruneNumericDVFieldName,
            boolean pruneIdField,
            CodecReader reader,
            BitSet recoverySourceToKeep
        ) {
            super(reader);
            this.pruneStoredFieldName = pruneStoredFieldName;
            this.recoverySourceToKeep = recoverySourceToKeep;
            this.pruneNumericDVFieldName = pruneNumericDVFieldName;
            this.pruneIdField = pruneIdField;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer docValuesReader = super.getDocValuesReader();
            return new FilterDocValuesProducer(docValuesReader) {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                    NumericDocValues numeric = super.getNumeric(field);
                    if (field.name.equals(pruneNumericDVFieldName)) {
                        assert numeric != null : pruneNumericDVFieldName + " must have numeric DV but was null";
                        final DocIdSetIterator intersection;
                        if (recoverySourceToKeep == null) {
                            // we can't return null here lucenes DocIdMerger expects an instance
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
            if (pruneStoredFieldName == null && pruneIdField == false) {
                // nothing to prune, we can use the original fields reader
                return super.getFieldsReader();
            }
            return new RecoverySourcePruningStoredFieldsReader(
                super.getFieldsReader(),
                recoverySourceToKeep,
                pruneStoredFieldName,
                pruneIdField
            );
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }

        private static class RecoverySourcePruningStoredFieldsReader extends FilterStoredFieldsReader {

            private final BitSet recoverySourceToKeep;
            private final String recoverySourceField;
            private final boolean pruneIdField;

            RecoverySourcePruningStoredFieldsReader(
                StoredFieldsReader in,
                BitSet recoverySourceToKeep,
                @Nullable String recoverySourceField,
                boolean pruneIdField
            ) {
                super(in);
                assert recoverySourceField != null || pruneIdField : "nothing to prune";
                this.recoverySourceToKeep = recoverySourceToKeep;
                this.recoverySourceField = recoverySourceField;
                this.pruneIdField = pruneIdField;
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                if (recoverySourceToKeep != null && recoverySourceToKeep.get(docID)) {
                    super.document(docID, visitor);
                } else {
                    super.document(docID, new FilterStoredFieldVisitor(visitor) {
                        @Override
                        public Status needsField(FieldInfo fieldInfo) throws IOException {
                            if (fieldInfo.name.equals(recoverySourceField)) {
                                return Status.NO;
                            }
                            if (pruneIdField && IdFieldMapper.NAME.equals(fieldInfo.name)) {
                                return Status.NO;
                            }
                            return super.needsField(fieldInfo);
                        }
                    });
                }
            }

            @Override
            public StoredFieldsReader getMergeInstance() {
                return new RecoverySourcePruningStoredFieldsReader(
                    in.getMergeInstance(),
                    recoverySourceToKeep,
                    recoverySourceField,
                    pruneIdField
                );
            }

            @Override
            public StoredFieldsReader clone() {
                return new RecoverySourcePruningStoredFieldsReader(in.clone(), recoverySourceToKeep, recoverySourceField, pruneIdField);
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
     * A {@link FilterCodecReader} that wraps a default {@link StoredFieldsReader} into a {@link SkipSyntheticIdFilterStoredFieldsReader}.
     */
    private static class SkipSyntheticIdFilterCodecReader extends FilterCodecReader {

        private SkipSyntheticIdFilterCodecReader(CodecReader in) {
            super(in);
        }

        @Override
        public StoredFieldsReader getFieldsReader() {
            return new SkipSyntheticIdFilterStoredFieldsReader(super.getFieldsReader());
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    /**
     * A {@link FilterStoredFieldsReader} that prevents {@link StoredFieldVisitor} from accessing synthetic id stored fields during merges.
     * This is useful to avoid synthetic ids to be materialized from doc values unnecessarily.
     */
    private static class SkipSyntheticIdFilterStoredFieldsReader extends FilterStoredFieldsReader {

        SkipSyntheticIdFilterStoredFieldsReader(StoredFieldsReader fieldsReader) {
            super(fieldsReader);
        }

        @Override
        public StoredFieldsReader clone() {
            return new SkipSyntheticIdFilterStoredFieldsReader(in.clone());
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            in.document(docID, new FilterStoredFieldVisitor(visitor) {
                @Override
                public Status needsField(FieldInfo fieldInfo) throws IOException {
                    if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                        return Status.NO;
                    }
                    return super.needsField(fieldInfo);
                }
            });
        }
    }
}
