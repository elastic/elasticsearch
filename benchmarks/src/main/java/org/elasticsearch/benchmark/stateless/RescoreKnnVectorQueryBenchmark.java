/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.Random;

/**
 * Benchmark for {@link RescoreKnnVectorQuery} against a stateless-simulated Directory.
 *
 * <p>The inner query is a {@link KnnFloatVectorQuery} (production-shape: ANN over the
 * quantized HNSW graph), wrapped by a rescore that re-scores the top {@code rescoreK}
 * candidates with full-precision vectors.
 *
 * <p>Sweeps via {@link Param}:
 * <ul>
 *   <li>{@code dims} — vector dimensionality (powers of 2 up to ~2048)</li>
 *   <li>{@code numDocs} — index size</li>
 *   <li>{@code k} — final result count</li>
 *   <li>{@code rescoreK} — number of candidates rescored with full-precision vectors</li>
 * </ul>
 *
 * <p>Cold/hot cache state and simulated first-byte latency are inherited @Params from
 * {@link AbstractStatelessQueryBenchmark}.
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class RescoreKnnVectorQueryBenchmark extends AbstractStatelessQueryBenchmark {

    private static final String FIELD = "vector";
    private static final long SEED = 42L;

    @Param({ "128", "1024", "2048" })
    public int dims;

    @Param({ "50000" })
    public int numDocs;

    @Param({ "10" })
    public int k;

    @Param({ "100" })
    public int rescoreK;

    private float[] queryVector;

    @Override
    protected IndexWriterConfig indexWriterConfig() {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES94HnswScalarQuantizedVectorsFormat();
            }
        });
        iwc.setUseCompoundFile(false);
        return iwc;
    }

    @Override
    protected void buildIndex(IndexWriter writer) throws IOException {
        Random rng = new Random(SEED);
        for (int i = 0; i < numDocs; i++) {
            float[] v = randomUnitVector(rng, dims);
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, v, VectorSimilarityFunction.DOT_PRODUCT));
            writer.addDocument(doc);
        }
        queryVector = randomUnitVector(rng, dims);
    }

    @Override
    protected Object runQuery(IndexSearcher searcher) throws IOException {
        Query inner = new KnnFloatVectorQuery(FIELD, queryVector, rescoreK);
        Query rescore = RescoreKnnVectorQuery.fromInnerQuery(FIELD, queryVector, k, rescoreK, inner);
        return searcher.search(rescore, k);
    }

    private static float[] randomUnitVector(Random rng, int dims) {
        float[] v = new float[dims];
        for (int d = 0; d < dims; d++) {
            v[d] = rng.nextFloat() * 2f - 1f;
        }
        VectorUtil.l2normalize(v);
        return v;
    }
}
