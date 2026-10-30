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
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.field.vectors.FloatRankVectorsDocValuesField;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.KnnScoreDocQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Uses a MUVERA FDE to select candidates and replaces every approximate score
 * with exact MaxSim over the original rank vectors before returning any hits.
 */
final class MuveraMaxSimQuery extends Query {

    static final int MAX_CANDIDATES = 10_000;
    static final long MAX_EXACT_DOT_PRODUCTS = 10_000_000L;
    private static final Runnable NO_CANCELLATION = () -> {};

    private final String field;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;
    @Nullable
    private final Query filter;

    MuveraMaxSimQuery(String field, float[][] queryVectors, int k, int numCandidates, @Nullable Query filter) {
        if (queryVectors == null || queryVectors.length == 0) {
            throw new IllegalArgumentException("query vectors must not be empty");
        }
        if (queryVectors.length > RankVectorsFieldMapper.MAX_VECTORS) {
            throw new IllegalArgumentException(
                "query vector count [" + queryVectors.length + "] exceeds the maximum of [" + RankVectorsFieldMapper.MAX_VECTORS + "]"
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
        long exactDotProducts = Math.multiplyExact(
            Math.multiplyExact((long) numCandidates, RankVectorsFieldMapper.MAX_MUVERA_VECTORS),
            queryVectors.length
        );
        if (exactDotProducts > MAX_EXACT_DOT_PRODUCTS) {
            throw new IllegalArgumentException(
                "exact MaxSim work ["
                    + exactDotProducts
                    + " dot products] exceeds the maximum of ["
                    + MAX_EXACT_DOT_PRODUCTS
                    + "]; reduce [num_candidates] or the number of query vectors"
            );
        }
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.k = k;
        this.numCandidates = numCandidates;
        this.filter = filter;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Runnable checkCancelled = searcher instanceof ContextIndexSearcher contextSearcher
            ? contextSearcher::checkCancelled
            : NO_CANCELLATION;
        float[] queryFde = RankVectorsFieldMapper.encodeMuveraQuery(queryVectors, checkCancelled);
        TopDocs candidates = searcher.search(new KnnFloatVectorQuery(field, queryFde, numCandidates, filter), numCandidates);
        if (candidates.scoreDocs.length == 0) {
            return new KnnScoreDocQuery(new ScoreDoc[0], searcher.getIndexReader());
        }

        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        BinaryDocValues[] values = new BinaryDocValues[leaves.size()];
        List<ScoreDoc> rescored = new ArrayList<>(candidates.scoreDocs.length);
        float[] maximums = new float[queryVectors.length];
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
                float rawMaxSim = exactMaxSim(
                    queryVectors,
                    binary.binaryValue(),
                    RankVectorsFieldMapper.MUVERA_TOKEN_DIMS,
                    maximums,
                    checkCancelled
                );
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
        return exactMaxSim(queryVectors, documentVectors, dimensions, new float[queryVectors.length], NO_CANCELLATION);
    }

    static float exactMaxSim(float[][] queryVectors, BytesRef documentVectors, int dimensions, float[] maximums, Runnable checkCancelled) {
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
        Arrays.fill(maximums, Float.NEGATIVE_INFINITY);
        var vectors = new FloatRankVectorsDocValuesField.FloatVectorIterator(documentVectors, new float[dimensions], documentVectorCount);
        while (vectors.hasNext()) {
            checkCancelled.run();
            float[] documentVector = vectors.next();
            for (int query = 0; query < queryVectors.length; query++) {
                maximums[query] = Math.max(maximums[query], VectorUtil.dotProduct(queryVectors[query], documentVector));
            }
        }
        float result = 0f;
        for (float maximum : maximums) {
            result += maximum;
        }
        return result;
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
            && Objects.equals(filter, other.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, Arrays.deepHashCode(queryVectors), k, numCandidates, filter);
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
