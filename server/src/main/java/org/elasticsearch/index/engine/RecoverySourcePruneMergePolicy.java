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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.function.Supplier;

final class RecoverySourcePruneMergePolicy extends OneMergeWrappingMergePolicy {
    RecoverySourcePruneMergePolicy(String recoverySourceField, Supplier<Query> retentionPolicySupplier, MergePolicy in) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                NumericDocValues recovery_source = wrapped.getNumericDocValues(recoverySourceField);
                if (recovery_source == null || recovery_source.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    return wrapped;
                }
                return wrapReader(recoverySourceField, wrapped, retentionPolicySupplier);
            }
        });

    }

    // pkg private for testing
    static CodecReader wrapReader(String recoverySourceField, CodecReader reader, Supplier<Query> retentionPolicySupplier) throws IOException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new DocValuesFieldExistsQuery(recoverySourceField), BooleanClause.Occur.FILTER);
        builder.add(retentionPolicySupplier.get(), BooleanClause.Occur.FILTER);
        IndexSearcher s = new IndexSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(builder.build(), false, 1.0f);
        Scorer scorer = weight.scorer(reader.getContext());

        if (scorer != null) {
            BitSet sourceToDrop = BitSet.of(scorer.iterator(), reader.maxDoc());
            return new SourcePruningFilterCodecReader(recoverySourceField, reader, sourceToDrop);
        } else {
            return reader;
        }
    }

    private static class SourcePruningFilterCodecReader extends FilterCodecReader {
        private final BitSet sourceToDrop;
        private final String recoverySourceField;

        public SourcePruningFilterCodecReader(String recoverySourceField, CodecReader reader, BitSet sourceToDrop) {
            super(reader);
            this.recoverySourceField = recoverySourceField;
            this.sourceToDrop = sourceToDrop;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer docValuesReader = super.getDocValuesReader();
            return new FilterDocValuesProducer(docValuesReader) {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                    NumericDocValues numeric = super.getNumeric(field);
                    if (recoverySourceField.equals(field.name)) {
                        return new FilterNumericDocValues(numeric) {
                            @Override
                            public int nextDoc() throws IOException {
                                int doc;
                                do {
                                    doc = super.nextDoc();
                                } while (doc != NO_MORE_DOCS && sourceToDrop.get(doc));
                                return doc;
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
                    if (sourceToDrop.get(docID)) {
                        visitor = new FilterStoredFieldVisitor(visitor) {
                            @Override
                            public Status needsField(FieldInfo fieldInfo) throws IOException {
                                if (recoverySourceField.equals(fieldInfo.name)) {
                                    return Status.NO;
                                }
                                return super.needsField(fieldInfo);
                            }
                        };
                    }
                    super.visitDocument(docID, visitor);
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
