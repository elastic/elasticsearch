/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact knn query. Will iterate and score all documents that have the provided dense vector field in
 * the index. An optional filter restricts scoring to documents that also match that query.
 *
 * <p>Each subclass takes an optional {@link VectorSimilarityFunction}, which selects one of three modes:
 * <ul>
 *     <li>{@code function == null}: scoring uses the codec-bound scorer returned by
 *     {@code vectorValues.scorer(query)}. On codec-quantized fields this scores against the
 *     quantized representation.</li>
 *     <li>{@code function != null} and equal to the field's bound similarity function: scoring uses
 *     {@code vectorValues.rescorer(query)}, Lucene's primitive for highest-fidelity raw scoring.
 *     For codecs that preserve raw vectors alongside quantized ones (INT8/INT4/BBQ via
 *     {@code QuantizedAndRawFloatVectorValues} and friends) this returns the raw scorer; for
 *     non-quantized codecs it equals {@code scorer(query)} per the Lucene default.</li>
 *     <li>{@code function != null} and different from the field's bound similarity function:
 *     scoring iterates {@code vectorValue(ord)} and applies {@code function.compare(target, raw)}
 *     directly — the only path that supports a per-query similarity-function override.</li>
 * </ul>
 *
 * <p>Non-indexed (index:false) fields have no KNN vector values; their vectors are stored as binary doc
 * values. The doc-values constructors select a scorer that decodes each document's vector and applies a
 * (always non-null) {@code function} directly. There is no codec or quantized representation in this mode.
 *
 * <p>{@link ScorerSupplier#bulkScorer()} is overridden to return a {@code DenseVectorBulkScorer}
 * that drives the top-level collection path. It calls {@code DenseVectorScorer#nextDocsAndScores}
 * in a loop, which delegates to {@link VectorScorer#bulk}, letting similarity computation run in
 * SIMD-friendly batches without per-document dispatch overhead.
 *
 * <p>{@link ScorerSupplier#get(long)} returns a {@code DenseVectorScorer} that also supports
 * per-document {@link Scorer#score()} for the explain and conjunction paths.
 */
public abstract class DenseVectorQuery extends Query {

    protected final String field;
    protected final Query filter;

    public DenseVectorQuery(String field, Query filter) {
        this.field = field;
        this.filter = filter;
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (filter != null && filter.getClass() != MatchAllDocsQuery.class) {
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER).build();
            Query rewritten = searcher.rewrite(booleanQuery);
            return rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else {
            // If the filter is a match all docs query, we can skip it
            return null;
        }
    }

    abstract static class DenseVectorWeight extends Weight {
        private final String field;
        private final float boost;
        private final Weight filterWeight;

        protected DenseVectorWeight(DenseVectorQuery query, float boost, Weight filterWeight) {
            super(query);
            this.field = query.field;
            this.boost = boost;
            this.filterWeight = filterWeight;
        }

        abstract VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException;

        @Override
        public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
            if (filterWeight != null) {
                Explanation filterExplanation = filterWeight.explain(leafReaderContext, i);
                if (filterExplanation.isMatch() == false) {
                    return Explanation.noMatch("Document does not match filter", filterExplanation);
                }
            }
            VectorScorer vectorScorer = vectorScorer(leafReaderContext);
            if (vectorScorer == null) {
                return Explanation.noMatch("No vector values found for field: " + field);
            }
            DocIdSetIterator iterator = vectorScorer.iterator();
            iterator.advance(i);
            if (iterator.docID() == i) {
                float score = vectorScorer.score();
                return Explanation.match(score * boost, "found vector with calculated similarity: " + score);
            }
            return Explanation.noMatch("Document not found in vector values for field: " + field);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            VectorScorer vectorScorer = vectorScorer(context);
            if (vectorScorer == null) {
                return null;
            }
            final DocIdSetIterator filterIterator;
            final long cost;
            if (filterWeight != null) {
                ScorerSupplier filterSupplier = filterWeight.scorerSupplier(context);
                if (filterSupplier == null) {
                    return null;
                }
                filterIterator = filterSupplier.get(Long.MAX_VALUE).iterator();
                cost = Math.min(vectorScorer.iterator().cost(), filterIterator.cost());
            } else {
                filterIterator = null;
                cost = vectorScorer.iterator().cost();
            }
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    return new DenseVectorScorer(vectorScorer, filterIterator, boost);
                }

                @Override
                public BulkScorer bulkScorer() throws IOException {
                    return new DenseVectorBulkScorer(get(Long.MAX_VALUE), cost);
                }

                @Override
                public long cost() {
                    return cost;
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext leafReaderContext) {
            return true;
        }
    }

    public static class Floats extends DenseVectorQuery {

        private final float[] query;
        private final VectorSimilarityFunction function;
        // Non-null only for non-indexed (index:false) fields, which are scored from binary doc values; see the
        // doc-values constructor. elementType selects float vs bfloat16 decoding.
        private final ElementType docValuesElementType;
        private final IndexVersion docValuesIndexVersion;

        /**
         * Codec-bound scoring (uses {@code FloatVectorValues.scorer(query)}). On quantized fields
         * this scores against the quantized representation.
         */
        public Floats(float[] query, String field, Query filter) {
            this(query, field, filter, null);
        }

        /**
         * Raw scoring with the given {@code function}. When {@code function} matches the field's bound
         * similarity, scoring uses {@link FloatVectorValues#rescorer(float[])} (Lucene's high-fidelity
         * raw primitive); when it differs, scoring iterates {@code vectorValue(ord)} and applies
         * {@code function.compare(query, raw)} directly. Pass {@code null} to use the codec scorer.
         */
        public Floats(float[] query, String field, Query filter, VectorSimilarityFunction function) {
            this(query, field, filter, function, null, null);
        }

        /**
         * Scores a non-indexed (index:false) field from binary doc values, decoding each document's vector
         * (per {@code elementType}) and applying {@code function}. Use only when the field has no KNN values.
         */
        public Floats(
            float[] query,
            String field,
            Query filter,
            VectorSimilarityFunction function,
            ElementType elementType,
            IndexVersion indexVersion
        ) {
            super(field, filter);
            this.query = query;
            this.function = function;
            this.docValuesElementType = elementType;
            this.docValuesIndexVersion = indexVersion;
        }

        public float[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Floats";
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            if (filter == null) return this;
            Query rewritten = indexSearcher.rewrite(filter);
            if (rewritten == filter) {
                return this;
            } else if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            } else {
                return new Floats(query, field, rewritten, function, docValuesElementType, docValuesIndexVersion);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight filterWeight = super.createWeight(searcher, scoreMode, boost);
            return new DenseVectorWeight(Floats.this, boost, filterWeight) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    if (docValuesIndexVersion != null) {
                        BinaryDocValues docValues = leafReaderContext.reader().getBinaryDocValues(field);
                        if (docValues == null) {
                            return null;
                        }
                        return new DocValuesFloatVectorScorer(docValues, query, function, docValuesElementType, docValuesIndexVersion);
                    }
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    if (function == null) {
                        return vectorValues.scorer(query);
                    }
                    FieldInfo fieldInfo = leafReaderContext.reader().getFieldInfos().fieldInfo(field);
                    if (fieldInfo != null && fieldInfo.getVectorSimilarityFunction() == function) {
                        return vectorValues.rescorer(query);
                    }
                    return new RawFloatVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats other = (Floats) o;
            return Objects.equals(field, other.field)
                && Objects.deepEquals(query, other.query)
                && Objects.equals(filter, other.filter)
                && function == other.function
                && docValuesElementType == other.docValuesElementType
                && Objects.equals(docValuesIndexVersion, other.docValuesIndexVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), filter, function, docValuesElementType, docValuesIndexVersion);
        }

        /** Decodes each document's float vector from binary doc values and applies {@code function}. */
        private static final class DocValuesFloatVectorScorer implements VectorScorer {
            private final BinaryDocValues values;
            private final float[] target;
            private final VectorSimilarityFunction function;
            private final ElementType elementType;
            private final IndexVersion indexVersion;
            private final float[] decoded;

            DocValuesFloatVectorScorer(
                BinaryDocValues values,
                float[] target,
                VectorSimilarityFunction function,
                ElementType elementType,
                IndexVersion indexVersion
            ) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.elementType = elementType;
                this.indexVersion = indexVersion;
                this.decoded = new float[target.length];
            }

            @Override
            public float score() throws IOException {
                BytesRef ref = values.binaryValue();
                if (elementType == ElementType.BFLOAT16) {
                    VectorEncoderDecoder.decodeBFloat16DenseVector(ref, decoded);
                } else {
                    VectorEncoderDecoder.decodeDenseVector(indexVersion, ref, decoded);
                }
                return function.compare(target, decoded);
            }

            @Override
            public DocIdSetIterator iterator() {
                return values;
            }
        }

        private static final class RawFloatVectorScorer implements VectorScorer {
            private final FloatVectorValues values;
            private final float[] target;
            private final VectorSimilarityFunction function;
            private final KnnVectorValues.DocIndexIterator iterator;

            RawFloatVectorScorer(FloatVectorValues values, float[] target, VectorSimilarityFunction function) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.iterator = values.iterator();
            }

            @Override
            public float score() throws IOException {
                return function.compare(target, values.vectorValue(iterator.index()));
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }
        }
    }

    public static class Bytes extends DenseVectorQuery {

        private final byte[] query;
        private final VectorSimilarityFunction function;
        // Non-null only for non-indexed (index:false) byte fields, which are scored from binary doc values;
        // see the doc-values constructor.
        private final IndexVersion docValuesIndexVersion;

        /**
         * Codec-bound scoring (uses {@code ByteVectorValues.scorer(query)}).
         */
        public Bytes(byte[] query, String field, Query filter) {
            this(query, field, filter, null);
        }

        /**
         * Raw scoring with the given {@code function}. When {@code function} matches the field's bound
         * similarity, scoring uses {@link ByteVectorValues#rescorer(byte[])} (Lucene's high-fidelity
         * raw primitive); when it differs, scoring iterates {@code vectorValue(ord)} and applies
         * {@code function.compare(query, raw)} directly. Pass {@code null} to use the codec scorer.
         */
        public Bytes(byte[] query, String field, Query filter, VectorSimilarityFunction function) {
            this(query, field, filter, function, null);
        }

        /**
         * Scores a non-indexed (index:false) byte field from binary doc values, decoding each document's
         * vector and applying {@code function}. Use only when the field has no KNN values.
         */
        public Bytes(byte[] query, String field, Query filter, VectorSimilarityFunction function, IndexVersion indexVersion) {
            super(field, filter);
            this.query = query;
            this.function = function;
            this.docValuesIndexVersion = indexVersion;
        }

        public byte[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Bytes";
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            if (filter == null) return this;
            Query rewritten = indexSearcher.rewrite(filter);
            if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            } else if (rewritten == filter) {
                return this;
            } else {
                return new Bytes(query, field, rewritten, function, docValuesIndexVersion);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight filterWeight = super.createWeight(searcher, scoreMode, boost);
            return new DenseVectorWeight(Bytes.this, boost, filterWeight) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    if (docValuesIndexVersion != null) {
                        BinaryDocValues docValues = leafReaderContext.reader().getBinaryDocValues(field);
                        if (docValues == null) {
                            return null;
                        }
                        return new DocValuesByteVectorScorer(docValues, query, function, docValuesIndexVersion);
                    }
                    ByteVectorValues vectorValues = leafReaderContext.reader().getByteVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    if (function == null) {
                        return vectorValues.scorer(query);
                    }
                    FieldInfo fieldInfo = leafReaderContext.reader().getFieldInfos().fieldInfo(field);
                    if (fieldInfo != null && fieldInfo.getVectorSimilarityFunction() == function) {
                        return vectorValues.rescorer(query);
                    }
                    return new RawByteVectorScorer(vectorValues, query, function);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes other = (Bytes) o;
            return Objects.equals(field, other.field)
                && Objects.deepEquals(query, other.query)
                && Objects.equals(filter, other.filter)
                && function == other.function
                && Objects.equals(docValuesIndexVersion, other.docValuesIndexVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), filter, function, docValuesIndexVersion);
        }

        /** Decodes each document's byte vector from binary doc values and applies {@code function}. */
        private static final class DocValuesByteVectorScorer implements VectorScorer {
            private final BinaryDocValues values;
            private final byte[] target;
            private final VectorSimilarityFunction function;
            private final IndexVersion indexVersion;
            private final byte[] decoded;

            DocValuesByteVectorScorer(BinaryDocValues values, byte[] target, VectorSimilarityFunction function, IndexVersion indexVersion) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.indexVersion = indexVersion;
                this.decoded = new byte[target.length];
            }

            @Override
            public float score() throws IOException {
                VectorEncoderDecoder.decodeDenseVector(indexVersion, values.binaryValue(), decoded);
                return function.compare(target, decoded);
            }

            @Override
            public DocIdSetIterator iterator() {
                return values;
            }
        }

        private static final class RawByteVectorScorer implements VectorScorer {
            private final ByteVectorValues values;
            private final byte[] target;
            private final VectorSimilarityFunction function;
            private final KnnVectorValues.DocIndexIterator iterator;

            RawByteVectorScorer(ByteVectorValues values, byte[] target, VectorSimilarityFunction function) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.iterator = values.iterator();
            }

            @Override
            public float score() throws IOException {
                return function.compare(target, values.vectorValue(iterator.index()));
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }
        }
    }

    static class DenseVectorScorer extends Scorer {

        private final VectorScorer vectorScorer;
        private final VectorScorer.Bulk bulkScorer;
        private final DocIdSetIterator iterator;
        private final float boost;

        DenseVectorScorer(VectorScorer vectorScorer, DocIdSetIterator filterIterator, float boost) throws IOException {
            this.vectorScorer = vectorScorer;
            // The bulkScorer does not give us access to its internal iterator, so we build our own conjunction here
            // that is mainly used for advancing and retrieving docID.
            this.bulkScorer = vectorScorer.bulk(filterIterator);
            this.iterator = filterIterator == null
                ? vectorScorer.iterator()
                : ConjunctionUtils.intersectIterators(List.of(vectorScorer.iterator(), filterIterator));
            this.boost = boost;
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }

        @Override
        public float getMaxScore(int i) throws IOException {
            // TODO: can we optimize this at all?
            return Float.POSITIVE_INFINITY;
        }

        @Override
        public float score() throws IOException {
            assert iterator.docID() != -1;
            return vectorScorer.score() * boost;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }

        @Override
        public void nextDocsAndScores(int upTo, Bits liveDocs, DocAndFloatFeatureBuffer buffer) throws IOException {
            bulkScorer.nextDocsAndScores(upTo, liveDocs, buffer);
            for (int i = 0; i < buffer.size; i++) {
                buffer.features[i] *= boost;
            }
        }
    }

    static class DenseVectorBulkScorer extends BulkScorer {
        private final DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
        private final Scorer scorer;
        private final long cost;
        private float currentScore;
        private final Scorable scorable = new Scorable() {
            @Override
            public float score() {
                return currentScore;
            }
        };

        DenseVectorBulkScorer(Scorer scorer, long cost) {
            this.scorer = scorer;
            this.cost = cost;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            collector.setScorer(scorable);

            if (scorer.docID() < min) {
                scorer.iterator().advance(min);
            }

            for (scorer.nextDocsAndScores(max, acceptDocs, buffer); buffer.size > 0; scorer.nextDocsAndScores(max, acceptDocs, buffer)) {
                for (int i = 0; i < buffer.size; i++) {
                    int doc = buffer.docs[i];
                    // currentScore is closed over by scorable, which is available to the collector
                    currentScore = buffer.features[i];
                    collector.collect(doc);
                }
            }

            return scorer.docID();
        }

        @Override
        public long cost() {
            return cost;
        }
    }
}
