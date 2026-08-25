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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenormalizedCosineFloatVectorValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact knn query. Will iterate and score all documents that have the provided dense vector field in
 * the index. This query never filters on its own; {@link FilteredDenseVectorQuery} wraps an instance of
 * this class to additionally restrict scoring to documents that match a filter query.
 *
 * <p>Each concrete subclass corresponds to exactly one way of producing a {@link VectorScorer}, chosen once
 * at construction time by the caller (see {@code DenseVectorFieldMapper}):
 * <ul>
 *     <li>{@link Floats#codecScored} / {@link Bytes#codecScored}: scoring uses the codec-bound scorer
 *     returned by {@code vectorValues.scorer(query)}. On codec-quantized fields this scores against the
 *     quantized representation.</li>
 *     <li>{@link Floats#rawScored} / {@link Bytes#rawScored}: raw scoring with an explicit
 *     {@link VectorSimilarityFunction}. Per leaf, this still prefers {@code vectorValues.rescorer(query)}
 *     when the requested function equals that leaf's bound similarity function — Lucene's primitive for
 *     highest-fidelity raw scoring, returning the raw (unquantized) scorer for codecs that preserve one
 *     (INT8/INT4/BBQ) — falling back to iterating {@code vectorValue(ord)} and applying
 *     {@code function.compare(target, raw)} directly otherwise. That per-leaf choice can't be hoisted to
 *     construction time: it depends on each segment's {@link FieldInfo}, which can differ across segments
 *     written before and after a mapping update. {@link Floats#rawScored} additionally supports
 *     {@code denormalize}: when the KNN-indexed vectors are unit-normalized but the query requests a
 *     different metric, the scorer reads {@code <field>._magnitude} {@link NumericDocValues} to reconstruct
 *     the original vectors before comparing.</li>
 *     <li>{@link DocValuesFloats} / {@link DocValuesBytes}: non-indexed ({@code index:false}) fields have no
 *     KNN vector values; their vectors are stored as binary doc values instead. These decode each document's
 *     vector and apply a (always non-null, except for bit vectors) {@code function} directly. There is no
 *     codec or quantized representation in this mode.</li>
 * </ul>
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

    public DenseVectorQuery(String field) {
        this.field = field;
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    @Override
    public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return denseVectorWeight(boost, null);
    }

    /** Wraps this query with {@code filter}, or returns this query unchanged if {@code filter} is null. */
    public Query filteredBy(Query filter) {
        return filter != null ? new FilteredDenseVectorQuery(this, filter) : this;
    }

    /**
     * Builds the weight that scores this query's vectors, restricted to documents that also match
     * {@code filterWeight} when non-null. {@link FilteredDenseVectorQuery} supplies the filterWeight;
     * the unfiltered {@link #createWeight} path above passes {@code null}.
     */
    DenseVectorWeight denseVectorWeight(float boost, Weight filterWeight) {
        return new DenseVectorWeight(this, boost, filterWeight);
    }

    /**
     * Produces the scorer for a single leaf. Each subclass implements exactly one of the cases documented
     * on the class: doc-values decode, codec-bound scoring, or raw scoring (with its per-leaf rescorer
     * preference). Returns {@code null} when the leaf has no vector values for {@link #field}.
     */
    abstract VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException;

    /**
     * Indicates if weight is cacheable given the query. Doc values require a careful override due to them being updatable.
     */
    boolean isCacheable(LeafReaderContext leafReaderContext) {
        return true;
    }

    static class DenseVectorWeight extends Weight {
        private final DenseVectorQuery query;
        private final String field;
        private final float boost;
        private final Weight filterWeight;

        protected DenseVectorWeight(DenseVectorQuery query, float boost, Weight filterWeight) {
            super(query);
            this.query = query;
            this.field = query.field;
            this.boost = boost;
            this.filterWeight = filterWeight;
        }

        @Override
        public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
            if (filterWeight != null) {
                Explanation filterExplanation = filterWeight.explain(leafReaderContext, i);
                if (filterExplanation.isMatch() == false) {
                    return Explanation.noMatch("Document does not match filter", filterExplanation);
                }
            }
            VectorScorer vectorScorer = query.vectorScorer(leafReaderContext);
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
            VectorScorer vectorScorer = query.vectorScorer(context);
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
            return query.isCacheable(leafReaderContext) && (filterWeight == null || filterWeight.isCacheable(leafReaderContext));
        }
    }

    public static class Floats extends DenseVectorQuery {

        private final float[] query;
        private final VectorSimilarityFunction function;
        // True when the KNN-indexed vectors are unit-normalized but the query requests a different metric.
        // The scorer reads <field>._magnitude NumericDocValues to reconstruct the originals before scoring.
        private final boolean denormalize;

        /**
         * Codec-bound scoring (uses {@code FloatVectorValues.scorer(query)}). On quantized fields this
         * scores against the quantized representation.
         */
        public static Floats codecScored(float[] query, String field) {
            return new Floats(query, field, null, false);
        }

        /**
         * Raw scoring with the given {@code function}. Per leaf, prefers {@code FloatVectorValues.rescorer(
         * query)} when {@code function} equals that leaf's bound similarity function; otherwise iterates
         * {@code vectorValue(ord)} and applies {@code function.compare} directly. When {@code denormalize} is
         * {@code true}, reads {@code <field>._magnitude} NumericDocValues to reconstruct original vectors
         * from unit-normalized KNN storage before applying {@code function}.
         */
        public static Floats rawScored(float[] query, String field, VectorSimilarityFunction function, boolean denormalize) {
            return new Floats(query, field, Objects.requireNonNull(function), denormalize);
        }

        private Floats(float[] query, String field, VectorSimilarityFunction function, boolean denormalize) {
            super(field);
            this.query = query;
            this.function = function;
            this.denormalize = denormalize;
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
        VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
            FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
            if (vectorValues == null) {
                return null;
            }
            if (function == null) {
                return vectorValues.scorer(query);
            }
            if (denormalize) {
                NumericDocValues magnitudes = leafReaderContext.reader()
                    .getNumericDocValues(field + DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX);
                DenormalizedCosineFloatVectorValues values = new DenormalizedCosineFloatVectorValues(vectorValues, magnitudes);
                return new RawVectorScorer<>(values, values::vectorValue, query, function::compare);
            }
            FieldInfo fieldInfo = leafReaderContext.reader().getFieldInfos().fieldInfo(field);
            if (fieldInfo != null && fieldInfo.getVectorSimilarityFunction() == function) {
                return vectorValues.rescorer(query);
            }
            return new RawVectorScorer<>(vectorValues, vectorValues::vectorValue, query, function::compare);
        }

        @Override
        boolean isCacheable(LeafReaderContext leafReaderContext) {
            return denormalize == false
                || DocValues.isCacheable(leafReaderContext, field + DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats other = (Floats) o;
            return Objects.equals(field, other.field)
                && Objects.deepEquals(query, other.query)
                && function == other.function
                && denormalize == other.denormalize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function, denormalize);
        }
    }

    public static class Bytes extends DenseVectorQuery {

        private final byte[] query;
        private final VectorSimilarityFunction function;

        /**
         * Codec-bound scoring (uses {@code ByteVectorValues.scorer(query)}).
         */
        public static Bytes codecScored(byte[] query, String field) {
            return new Bytes(query, field, null);
        }

        /**
         * Raw scoring with the given {@code function}. Per leaf, prefers {@link ByteVectorValues#rescorer(
         * byte[])} (Lucene's high-fidelity raw primitive) when {@code function} equals that leaf's bound
         * similarity function; otherwise iterates {@code vectorValue(ord)} and applies
         * {@code function.compare(query, raw)} directly.
         */
        public static Bytes rawScored(byte[] query, String field, VectorSimilarityFunction function) {
            return new Bytes(query, field, Objects.requireNonNull(function));
        }

        private Bytes(byte[] query, String field, VectorSimilarityFunction function) {
            super(field);
            this.query = query;
            this.function = function;
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
        VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
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
            return new RawVectorScorer<>(vectorValues, vectorValues::vectorValue, query, function::compare);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes other = (Bytes) o;
            return Objects.equals(field, other.field) && Objects.deepEquals(query, other.query) && function == other.function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function);
        }
    }

    private static final class RawVectorScorer<T> implements VectorScorer {
        @FunctionalInterface
        interface VectorLookup<T> {
            T vectorValue(int ord) throws IOException;
        }

        @FunctionalInterface
        interface Compare<T> {
            float apply(T a, T b);
        }

        private final VectorLookup<T> lookup;
        private final T target;
        private final Compare<T> compare;
        private final KnnVectorValues.DocIndexIterator iterator;

        RawVectorScorer(KnnVectorValues values, VectorLookup<T> lookup, T target, Compare<T> compare) {
            this.lookup = lookup;
            this.target = target;
            this.compare = compare;
            this.iterator = values.iterator();
        }

        @Override
        public float score() throws IOException {
            return compare.apply(target, lookup.vectorValue(iterator.index()));
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }
    }

    /**
     * Scores a non-indexed ({@code index:false}) float or bfloat16 field from binary doc values, decoding
     * each document's vector (per {@code elementType}) and applying {@code function}. Use only when the
     * field has no KNN vector values.
     */
    public static class DocValuesFloats extends DenseVectorQuery {

        private final float[] query;
        private final VectorSimilarityFunction function;
        private final ElementType elementType;
        private final IndexVersion indexVersion;

        public DocValuesFloats(
            float[] query,
            String field,
            VectorSimilarityFunction function,
            ElementType elementType,
            IndexVersion indexVersion
        ) {
            super(field);
            this.query = query;
            this.function = Objects.requireNonNull(function);
            this.elementType = elementType;
            this.indexVersion = indexVersion;
        }

        public float[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.DocValuesFloats";
        }

        @Override
        VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
            BinaryDocValues docValues = leafReaderContext.reader().getBinaryDocValues(field);
            if (docValues == null) {
                return null;
            }
            return new DocValuesFloatVectorScorer(docValues, query, function, elementType, indexVersion);
        }

        @Override
        boolean isCacheable(LeafReaderContext leafReaderContext) {
            return DocValues.isCacheable(leafReaderContext, field);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocValuesFloats other = (DocValuesFloats) o;
            return Objects.equals(field, other.field)
                && Objects.deepEquals(query, other.query)
                && function == other.function
                && elementType == other.elementType
                && Objects.equals(indexVersion, other.indexVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function, elementType, indexVersion);
        }

        /** Decodes each document's float vector from binary doc values and applies {@code function}. */
        private static final class DocValuesFloatVectorScorer implements VectorScorer {
            private final BinaryDocValues values;
            private final float[] target;
            private final VectorSimilarityFunction function;
            private final ElementType elementType;
            private final IndexVersion indexVersion;
            private final float[] decoded;
            // Non-zero when we can use the stored per-doc magnitude for COSINE scoring instead of
            // recomputing it. Only valid for FLOAT (not BFLOAT16, whose stored magnitude is computed
            // from the pre-encoding floats rather than the decoded bfloat16 values).
            private final float queryMagnitude;

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
                this.queryMagnitude = function == VectorSimilarityFunction.COSINE
                    && elementType == ElementType.FLOAT
                    && indexVersion.onOrAfter(DenseVectorFieldMapper.MAGNITUDE_STORED_INDEX_VERSION)
                        ? (float) Math.sqrt(VectorUtil.dotProduct(target, target))
                        : 0f;
            }

            @Override
            public float score() throws IOException {
                BytesRef ref = values.binaryValue();
                if (elementType == ElementType.BFLOAT16) {
                    VectorEncoderDecoder.decodeBFloat16DenseVector(ref, decoded);
                } else {
                    VectorEncoderDecoder.decodeDenseVector(indexVersion, ref, decoded);
                }
                if (queryMagnitude > 0f) {
                    float storedMagnitude = VectorEncoderDecoder.decodeMagnitude(indexVersion, ref);
                    float rawScore = VectorUtil.dotProduct(target, decoded) / (queryMagnitude * storedMagnitude);
                    return VectorUtil.normalizeToUnitInterval(rawScore);
                }
                return function.compare(target, decoded);
            }

            @Override
            public DocIdSetIterator iterator() {
                return values;
            }
        }
    }

    /**
     * Scores a non-indexed ({@code index:false}) byte or bit field from binary doc values, decoding each
     * document's vector. {@code byte} fields apply {@code function}; {@code bit} fields ({@code function ==
     * null}) score by Hamming distance instead. Use only when the field has no KNN vector values.
     */
    public static class DocValuesBytes extends DenseVectorQuery {

        private final byte[] query;
        private final VectorSimilarityFunction function;
        private final boolean isBit;

        public DocValuesBytes(byte[] query, String field, VectorSimilarityFunction function, boolean isBit) {
            super(field);
            this.query = query;
            this.function = function;
            this.isBit = isBit;
        }

        public byte[] getQuery() {
            return query;
        }

        public VectorSimilarityFunction getFunction() {
            return function;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.DocValuesBytes";
        }

        @Override
        VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
            BinaryDocValues docValues = leafReaderContext.reader().getBinaryDocValues(field);
            if (docValues == null) {
                return null;
            }
            return new DocValuesByteVectorScorer(docValues, query, function, isBit);
        }

        @Override
        boolean isCacheable(LeafReaderContext leafReaderContext) {
            return DocValues.isCacheable(leafReaderContext, field);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocValuesBytes other = (DocValuesBytes) o;
            return Objects.equals(field, other.field)
                && Objects.deepEquals(query, other.query)
                && function == other.function
                && isBit == other.isBit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), function, isBit);
        }

        /**
         * Decodes each document's vector from binary doc values. {@code byte} fields apply {@code function};
         * {@code bit} fields score by Hamming distance.
         */
        private static final class DocValuesByteVectorScorer implements VectorScorer {
            private final BinaryDocValues values;
            private final byte[] target;
            private final VectorSimilarityFunction function;
            private final boolean isBit;
            private final byte[] decoded;

            DocValuesByteVectorScorer(BinaryDocValues values, byte[] target, VectorSimilarityFunction function, boolean isBit) {
                this.values = values;
                this.target = target;
                this.function = function;
                this.isBit = isBit;
                this.decoded = new byte[target.length];
            }

            @Override
            public float score() throws IOException {
                VectorEncoderDecoder.decodeDenseVector(values.binaryValue(), decoded);
                if (isBit) {
                    return ESVectorUtil.hammingScore(target, decoded);
                }
                return function.compare(target, decoded);
            }

            @Override
            public DocIdSetIterator iterator() {
                return values;
            }
        }
    }

    static class DenseVectorScorer extends Scorer {

        private final VectorScorer vectorScorer;
        private final DocIdSetIterator filterIterator;
        // Must lazily initialize because the default VectorScorer#bulk method advances the iterator in a way that breaks conjunctions.
        // See https://github.com/apache/lucene/pull/16377.
        private VectorScorer.Bulk bulkScorer;
        private final DocIdSetIterator iterator;
        private final float boost;

        DenseVectorScorer(VectorScorer vectorScorer, DocIdSetIterator filterIterator, float boost) {
            this.vectorScorer = vectorScorer;
            this.filterIterator = filterIterator;
            // The bulkScorer does not give us access to its internal iterator, so we build our own conjunction here
            // that is mainly used for advancing and retrieving docID.
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
            if (bulkScorer == null) {
                bulkScorer = vectorScorer.bulk(filterIterator);
            }
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
