/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

final class RecoverySourcePruneMergePolicy extends OneMergeWrappingMergePolicy {
    RecoverySourcePruneMergePolicy(String recoverySourceField, Supplier<Query> retainSourceQuerySupplier, MergePolicy in) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(recoverySourceField, wrapped, retainSourceQuerySupplier);
            }
        });
    }

    private static CodecReader wrapReader(String recoverySourceField, CodecReader reader, Supplier<Query> retainSourceQuerySupplier)
        throws IOException {
        NumericDocValues recoverySource = reader.getNumericDocValues(recoverySourceField);
        if (recoverySource == null || recoverySource.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return reader; // early terminate - nothing to do here since non of the docs has a recovery source anymore.
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
                return reader; // keep all source
            }
            return new SourcePruningFilterCodecReader(recoverySourceField, reader, recoverySourceToKeep);
        } else {
            return new SourcePruningFilterCodecReader(recoverySourceField, reader, null);
        }
    }

    private static class SourcePruningFilterCodecReader extends FilterCodecReader {
        private final BitSet recoverySourceToKeep;
        private final String recoverySourceField;

        SourcePruningFilterCodecReader(String recoverySourceField, CodecReader reader, BitSet recoverySourceToKeep) {
            super(reader);
            this.recoverySourceField = recoverySourceField;
            this.recoverySourceToKeep = recoverySourceToKeep;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer docValuesReader = super.getDocValuesReader();
            return new FilterDocValuesProducer(docValuesReader) {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                    NumericDocValues numeric = super.getNumeric(field);
                    if (recoverySourceField.equals(field.name)) {
                        assert numeric != null : recoverySourceField + " must have numeric DV but was null";
                        final DocIdSetIterator intersection;
                        if (recoverySourceToKeep == null) {
                            // we can't return null here lucenes DocIdMerger expects an instance
                            intersection = DocIdSetIterator.empty();
                        } else {
                            intersection = ConjunctionDISI.intersectIterators(Arrays.asList(numeric,
                                new BitSetIterator(recoverySourceToKeep, recoverySourceToKeep.length())));
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
            return new FilterStoredFieldsReader(fieldsReader) {
                @Override
                public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                    if (recoverySourceToKeep != null && recoverySourceToKeep.get(docID)) {
                        super.visitDocument(docID, visitor);
                    } else {
                        super.visitDocument(docID, new FilterStoredFieldVisitor(visitor) {
                            @Override
                            public Status needsField(FieldInfo fieldInfo) throws IOException {
                                if (recoverySourceField.equals(fieldInfo.name)) {
                                    return Status.NO;
                                }
                                return super.needsField(fieldInfo);
                            }
                        });
                    }
                }
            };
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }

        private static class FilterDocValuesProducer extends DocValuesProducer {
            private final DocValuesProducer in;

            FilterDocValuesProducer(DocValuesProducer in) {
                this.in = in;
            }

            @Override
            public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                return in.getNumeric(field);
            }

            @Override
            public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                return in.getBinary(field);
            }

            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
                return in.getSorted(field);
            }

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return in.getSortedNumeric(field);
            }

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
                return in.getSortedSet(field);
            }

            @Override
            public void checkIntegrity() throws IOException {
                in.checkIntegrity();
            }

            @Override
            public void close() throws IOException {
                in.close();
            }

            @Override
            public long ramBytesUsed() {
                return in.ramBytesUsed();
            }
        }

        private static class FilterStoredFieldsReader extends StoredFieldsReader {

            private final StoredFieldsReader fieldsReader;

            FilterStoredFieldsReader(StoredFieldsReader fieldsReader) {
                this.fieldsReader = fieldsReader;
            }

            @Override
            public long ramBytesUsed() {
                return fieldsReader.ramBytesUsed();
            }

            @Override
            public void close() throws IOException {
                fieldsReader.close();
            }

            @Override
            public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                fieldsReader.visitDocument(docID, visitor);
            }

            @Override
            public StoredFieldsReader clone() {
                return fieldsReader.clone();
            }

            @Override
            public void checkIntegrity() throws IOException {
                fieldsReader.checkIntegrity();
            }
        }

        private static class FilterStoredFieldVisitor extends StoredFieldVisitor {
            private final StoredFieldVisitor visitor;

            FilterStoredFieldVisitor(StoredFieldVisitor visitor) {
                this.visitor = visitor;
            }

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                visitor.binaryField(fieldInfo, value);
            }

            @Override
            public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
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
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                return visitor.needsField(fieldInfo);
            }
        }
    }
}
