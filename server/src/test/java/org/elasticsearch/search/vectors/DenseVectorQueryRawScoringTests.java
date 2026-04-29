/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FullPrecisionFloatVectorSimilarityValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Verifies the two scoring modes of {@link DenseVectorQuery.Floats}:
 * <ol>
 *     <li><b>Raw path</b> (non-null {@link VectorSimilarityFunction}): scores must match Lucene's
 *     {@link FullPrecisionFloatVectorSimilarityValuesSource} (the canonical full-precision raw scorer)
 *     to within float-arithmetic tolerance — across every codec, including codec-quantized ones.
 *     This proves the raw path consults the preserved raw float values, not the quantized
 *     representation.</li>
 *     <li><b>Codec path</b> (null function): on quantized codecs (INT8_FLAT, INT8_HNSW), per-doc
 *     scores must stay close to the raw scores but differ measurably for at least one doc — proving
 *     the codec scorer exercises the quantized representation rather than the raw values.</li>
 * </ol>
 *
 * Each codec runs as its own parameterized invocation; the test infra reports per-codec results.
 */
public class DenseVectorQueryRawScoringTests extends ESTestCase {

    private static final String FIELD_NAME = "vector";
    private static final int NUM_DOCS = 30;
    private static final int NUM_DIMS = 16;
    /** Tolerance for "raw path matches the ground-truth values source" — float arithmetic noise only. */
    private static final double RAW_VS_GROUND_TRUTH_DELTA = 1e-5;

    @ParametersFactory(argumentFormatting = "codec=%s")
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(CodecCase.values()).map(c -> new Object[] { c }).toList();
    }

    private final CodecCase codec;

    public DenseVectorQueryRawScoringTests(@Name("codec") CodecCase codec) {
        this.codec = codec;
    }

    /**
     * The raw path must produce per-doc scores numerically equal to the ground-truth full-precision
     * scorer — even on codec-quantized indexes (which preserve raw float values via
     * {@code QuantizedAndRawFloatVectorValues}).
     */
    public void testRawPathMatchesGroundTruth() throws Exception {
        float[] queryVector = randomVector(NUM_DIMS);
        try (Directory d = newDirectory()) {
            indexRandomVectors(d, codec.format());

            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader, true, false);
                Map<Integer, Float> rawScores = scoreAll(
                    searcher,
                    new DenseVectorQuery.Floats(queryVector, FIELD_NAME, VectorSimilarityFunction.COSINE)
                );
                Map<Integer, Float> groundTruth = scoreAll(searcher, groundTruthQuery(queryVector));

                assertEquals("raw path covered different docs than ground truth", groundTruth.keySet(), rawScores.keySet());
                for (var entry : rawScores.entrySet()) {
                    assertThat(
                        "raw score mismatch for doc " + entry.getKey(),
                        (double) entry.getValue(),
                        closeTo(groundTruth.get(entry.getKey()), RAW_VS_GROUND_TRUTH_DELTA)
                    );
                }
            }
        }
    }

    /**
     * On a codec-quantized index, the codec-bound path must produce scores that stay within the
     * codec's quantization noise budget vs the raw path, but at least one doc must differ measurably.
     * This guarantees the codec path is genuinely scoring on the quantized representation.
     * Skipped for non-quantized codecs.
     */
    public void testCodecPathDiffersFromRawOnQuantizedCodecs() throws Exception {
        assumeTrue("non-quantized codec — codec path is identical to raw path", codec.expectsCodecDifference);
        float[] queryVector = randomVector(NUM_DIMS);
        try (Directory d = newDirectory()) {
            indexRandomVectors(d, codec.format());

            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader, true, false);
                Map<Integer, Float> rawScores = scoreAll(
                    searcher,
                    new DenseVectorQuery.Floats(queryVector, FIELD_NAME, VectorSimilarityFunction.COSINE)
                );
                Map<Integer, Float> codecScores = scoreAll(searcher, new DenseVectorQuery.Floats(queryVector, FIELD_NAME));

                assertEquals("codec path covered different docs than raw path", rawScores.keySet(), codecScores.keySet());
                double maxDelta = 0d;
                for (var entry : rawScores.entrySet()) {
                    double delta = Math.abs(entry.getValue() - codecScores.get(entry.getKey()));
                    maxDelta = Math.max(maxDelta, delta);
                    assertThat(
                        "codec score for doc " + entry.getKey() + " exceeds noise tolerance",
                        delta,
                        lessThanOrEqualTo(codec.codecVsRawTolerance)
                    );
                }
                assertThat(
                    "codec path produced identical scores to raw path; quantization not exercised",
                    maxDelta,
                    greaterThan(0d)
                );
            }
        }
    }

    private static FunctionScoreQuery groundTruthQuery(float[] queryVector) {
        DoubleValuesSource source = new FullPrecisionFloatVectorSimilarityValuesSource(
            queryVector,
            FIELD_NAME,
            VectorSimilarityFunction.COSINE
        );
        return new FunctionScoreQuery(Queries.ALL_DOCS_INSTANCE, source);
    }

    private static Map<Integer, Float> scoreAll(IndexSearcher searcher, Query query) throws IOException {
        TopDocs hits = searcher.search(query, NUM_DOCS);
        Map<Integer, Float> scores = new HashMap<>();
        for (ScoreDoc sd : hits.scoreDocs) {
            scores.put(sd.doc, sd.score);
        }
        return scores;
    }

    private static void indexRandomVectors(Directory d, KnnVectorsFormat format) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(new Elasticsearch93Lucene104Codec(Zstd814StoredFieldsFormat.Mode.BEST_SPEED) {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return format;
            }
        });
        try (IndexWriter w = new IndexWriter(d, iwc)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(FIELD_NAME, randomVector(NUM_DIMS), VectorSimilarityFunction.COSINE));
                w.addDocument(doc);
            }
            w.commit();
        }
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int j = 0; j < dims; j++) {
            vector[j] = randomFloatBetween(-1f, 1f, true);
        }
        return vector;
    }

    /** A codec to exercise plus the expected behavior of the codec-bound path on it. */
    public enum CodecCase {
        // Non-quantized codecs: raw path matches ground truth; codec path is not asserted to differ.
        FLAT(() -> new ES93FlatVectorFormat(ElementType.FLOAT), false, 0d),
        HNSW(() -> new ES93HnswVectorsFormat(ElementType.FLOAT), false, 0d),
        // Scalar-quantized codecs preserve raw vectors alongside int8 — raw path stays bit-equivalent
        // to ground truth, and the codec path must differ measurably (within int8 noise budget).
        INT8_FLAT(() -> new ES93ScalarQuantizedVectorsFormat(ElementType.FLOAT), true, 0.25),
        INT8_HNSW(ES93HnswScalarQuantizedVectorsFormat::new, true, 0.25);

        private final Supplier<KnnVectorsFormat> format;
        /** True if the codec applies quantization, so the codec-bound path must differ from raw. */
        private final boolean expectsCodecDifference;
        /** Per-doc tolerance for codec score vs raw score. */
        private final double codecVsRawTolerance;

        CodecCase(Supplier<KnnVectorsFormat> format, boolean expectsCodecDifference, double codecVsRawTolerance) {
            this.format = format;
            this.expectsCodecDifference = expectsCodecDifference;
            this.codecVsRawTolerance = codecVsRawTolerance;
        }

        KnnVectorsFormat format() {
            return format.get();
        }
    }
}
