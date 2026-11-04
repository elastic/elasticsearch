/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.field.vectors.FloatRankVectors;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.KnnScoreDocQuery;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;

/**
 * Uses a MUVERA FDE to select candidates and replaces every approximate score
 * with exact MaxSim over the original rank vectors before returning any hits.
 */
final class MuveraMaxSimQuery extends Query {

    static final int MAX_CANDIDATES = 10_000;
    private static final Runnable NO_CANCELLATION = () -> {};

    private final String field;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;
    @Nullable
    private final Float queryOversample;
    @Nullable
    private final Query filter;
    private final MuveraEncoder encoder;

    MuveraMaxSimQuery(String field, float[][] queryVectors, int k, int numCandidates, @Nullable Query filter, MuveraEncoder encoder) {
        this(field, queryVectors, k, numCandidates, null, filter, encoder);
    }

    MuveraMaxSimQuery(
        String field,
        float[][] queryVectors,
        int k,
        int numCandidates,
        @Nullable Float queryOversample,
        @Nullable Query filter,
        MuveraEncoder encoder
    ) {
        if (queryVectors == null || queryVectors.length == 0) {
            throw new IllegalArgumentException("query vectors must not be empty");
        }
        if (queryVectors.length > RankVectorsFieldMapper.MAX_MUVERA_VECTORS) {
            throw new IllegalArgumentException(
                "query vector count ["
                    + queryVectors.length
                    + "] exceeds the maximum of ["
                    + RankVectorsFieldMapper.MAX_MUVERA_VECTORS
                    + "]"
            );
        }
        if (k < 1) {
            throw new IllegalArgumentException("[k] must be positive");
        }
        if (numCandidates < k) {
            throw new IllegalArgumentException("[num_candidates] must be greater than or equal to [k]");
        }
        if (numCandidates > MAX_CANDIDATES) {
            throw new IllegalArgumentException("[num_candidates] cannot exceed [" + MAX_CANDIDATES + "]");
        }
        RankVectorsQueryBuilder.validateRescoreWork(
            queryVectors.length,
            k,
            numCandidates,
            queryOversample == null ? DEFAULT_OVERSAMPLE : queryOversample
        );
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.k = k;
        this.numCandidates = numCandidates;
        this.queryOversample = queryOversample;
        this.filter = filter;
        this.encoder = Objects.requireNonNull(encoder);
        this.encoder.validateQueryVectors(this.queryVectors);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Runnable checkCancelled = searcher instanceof ContextIndexSearcher contextSearcher
            ? contextSearcher::checkCancelled
            : NO_CANCELLATION;
        float[] queryFde = encoder.encodeQuery(queryVectors, checkCancelled);
        TopDocs candidates = searcher.search(
            MuveraCandidateQuery.approximateQuery(field, queryFde, k, numCandidates, queryOversample, filter),
            numCandidates
        );
        if (candidates.scoreDocs.length == 0) {
            return new KnnScoreDocQuery(new ScoreDoc[0], searcher.getIndexReader());
        }

        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        BinaryDocValues[] values = new BinaryDocValues[leaves.size()];
        List<ScoreDoc> rescored = new ArrayList<>(candidates.scoreDocs.length);
        ScoreDoc[] candidatesByDoc = candidates.scoreDocs.clone();
        Arrays.sort(candidatesByDoc, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        for (ScoreDoc candidate : candidatesByDoc) {
            checkCancelled.run();
            int leafIndex = ReaderUtil.subIndex(candidate.doc, leaves);
            LeafReaderContext leaf = leaves.get(leafIndex);
            BinaryDocValues binary = values[leafIndex];
            if (binary == null) {
                binary = DocValues.getBinary(leaf.reader(), field);
                values[leafIndex] = binary;
            }
            int segmentDoc = candidate.doc - leaf.docBase;
            if (binary.advanceExact(segmentDoc)) {
                float rawMaxSim = exactMaxSim(queryVectors, binary.binaryValue(), RankVectorsFieldMapper.MUVERA_TOKEN_DIMS, checkCancelled);
                rescored.add(new ScoreDoc(candidate.doc, score(rawMaxSim)));
            }
        }
        rescored.sort(
            Comparator.comparingDouble((ScoreDoc scoreDoc) -> scoreDoc.score).reversed().thenComparingInt(scoreDoc -> scoreDoc.doc)
        );
        int resultSize = Math.min(k, rescored.size());
        return new KnnScoreDocQuery(rescored.subList(0, resultSize).toArray(ScoreDoc[]::new), searcher.getIndexReader());
    }

    static float exactMaxSim(float[][] queryVectors, BytesRef documentVectors, int dimensions) {
        return exactMaxSim(queryVectors, documentVectors, dimensions, NO_CANCELLATION);
    }

    static float exactMaxSim(float[][] queryVectors, BytesRef documentVectors, int dimensions, Runnable checkCancelled) {
        int vectorBytes = Math.multiplyExact(dimensions, Float.BYTES);
        if (documentVectors.length == 0 || documentVectors.length % vectorBytes != 0) {
            throw new IllegalArgumentException("invalid rank_vectors binary doc value length [" + documentVectors.length + "]");
        }
        int documentVectorCount = documentVectors.length / vectorBytes;
        for (float[] queryVector : queryVectors) {
            if (queryVector.length != dimensions) {
                throw new IllegalArgumentException(
                    "query vector dimensions [" + queryVector.length + "] differ from field dimensions [" + dimensions + "]"
                );
            }
        }
        checkCancelled.run();
        float score = FloatRankVectors.maxSimDotProduct(documentVectors, documentVectorCount, dimensions, queryVectors);
        checkCancelled.run();
        return score;
    }

    /**
     * Lucene scores must be non-negative. This transform is strictly monotonic,
     * continuous at zero, and therefore preserves exact MaxSim ordering.
     */
    static float score(float rawMaxSim) {
        return rawMaxSim < 0f ? 1f / (1f - rawMaxSim) : rawMaxSim + 1f;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "MuveraMaxSimQuery(field=" + this.field + ", k=" + k + ", numCandidates=" + numCandidates + ")";
    }

    @Override
    public boolean equals(Object object) {
        if (sameClassAs(object) == false) {
            return false;
        }
        MuveraMaxSimQuery other = (MuveraMaxSimQuery) object;
        return field.equals(other.field)
            && Arrays.deepEquals(queryVectors, other.queryVectors)
            && k == other.k
            && numCandidates == other.numCandidates
            && Objects.equals(queryOversample, other.queryOversample)
            && Objects.equals(filter, other.filter)
            && encoder == other.encoder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, Arrays.deepHashCode(queryVectors), k, numCandidates, queryOversample, filter, encoder);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            if (vectors[i] == null) {
                throw new IllegalArgumentException("query vector [" + i + "] must not be null");
            }
            copy[i] = vectors[i].clone();
        }
        return copy;
    }

}
