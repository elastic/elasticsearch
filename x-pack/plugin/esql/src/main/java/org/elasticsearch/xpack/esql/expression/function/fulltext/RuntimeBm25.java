/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.similarity.LegacyBM25Similarity;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * BM25 scoring for a runtime {@code MATCH}, whose column has no inverted index to take collection statistics from.
 * The statistics ({@code N}, the summed length and each query term's document frequency) are computed by a separate
 * stats pass over the rows entering the {@code MATCH} (see {@link RuntimeTermStat}) and handed to the scorer through
 * {@link RuntimeBm25Field}. The per-row half (term frequencies and length) is computed here, while scoring.
 * <p>
 * Uses the similarity indexed text fields default to, and encodes lengths the way their norms do, so a runtime score is
 * the score the same text would get from an index holding exactly the rows that entered the {@code MATCH}.
 */
public final class RuntimeBm25 {
    static final LegacyBM25Similarity SIMILARITY = new LegacyBM25Similarity();

    private RuntimeBm25() {}

    /** Each distinct term of the analyzed query with the number of times it occurs, i.e. its weight. */
    public static Map<BytesRef, Integer> analyzeQuery(Analyzer analyzer, String query) {
        try {
            return RuntimeSearch.analyzeTermsWithCounts(analyzer, query);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to tokenize query string: " + e.getMessage(), e);
        }
    }

    /** The values analyzer a runtime text column declares, resolved on the executing node; standard when none is declared. */
    static Analyzer valuesAnalyzer(Expression field, EvaluatorMapper.ToEvaluator toEvaluator) {
        String name = AnalyzedTextExpression.valuesAnalyzerOf(field);
        return name == null ? new StandardAnalyzer() : RuntimeSearch.resolveNamedAnalyzer(name, toEvaluator);
    }

    /**
     * The per-row statistics BM25 needs, accumulated over all the values of a position (they form one document).
     */
    static final class RowStats {
        final int[] termFreqs;
        /** Every token: what the collection's summed length adds up. */
        int length;
        /** Tokens that advance the position: what the norm encodes, since BM25 discounts overlaps by default. */
        int normLength;

        RowStats(int termCount) {
            this.termFreqs = new int[termCount];
        }

        void reset() {
            Arrays.fill(termFreqs, 0);
            length = 0;
            normLength = 0;
        }

        void add(Analyzer analyzer, String value, Map<BytesRef, Integer> termIndex) {
            try (TokenStream stream = analyzer.tokenStream(RuntimeSearch.CONTENT_FIELD, value)) {
                stream.reset();
                TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
                PositionIncrementAttribute positionIncrement = stream.addAttribute(PositionIncrementAttribute.class);
                while (stream.incrementToken()) {
                    length++;
                    if (positionIncrement.getPositionIncrement() != 0) {
                        normLength++;
                    }
                    Integer index = termIndex.get(term.getBytesRef());
                    if (index != null) {
                        termFreqs[index]++;
                    }
                }
                stream.end();
            } catch (IOException e) {
                // Analyzing an in-memory string does no IO.
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * One scorer per query term, {@code null} for a term no row of the collection contains, or {@code null} altogether when the
     * collection is empty. The statistics come from a separate pass and may lag behind the scored rows (a file or an index can
     * change in between), so they are clamped into the ranges Lucene accepts instead of trusted.
     */
    @Nullable
    static SimScorer[] scorers(long docCount, long sumTotalTermFreq, long[] docFreqs, List<BytesRef> terms) {
        if (docCount <= 0) {
            return null;
        }
        CollectionStatistics collection = new CollectionStatistics(
            RuntimeSearch.CONTENT_FIELD,
            docCount,
            docCount,
            Math.max(sumTotalTermFreq, docCount),
            docCount
        );
        SimScorer[] scorers = new SimScorer[docFreqs.length];
        for (int i = 0; i < docFreqs.length; i++) {
            long docFreq = Math.min(docFreqs[i], docCount);
            if (docFreq > 0) {
                scorers[i] = SIMILARITY.scorer(1.0f, collection, new TermStatistics(terms.get(i), docFreq, docFreq));
            }
        }
        return scorers;
    }

    /** The row's score: the sum over the query terms it contains, each weighed by how often it occurs in the query. */
    static double score(RowStats row, int[] weights, SimScorer[] scorers) {
        long norm = SmallFloat.intToByte4(row.normLength);
        double score = 0;
        for (int i = 0; i < weights.length; i++) {
            if (row.termFreqs[i] > 0 && scorers[i] != null) {
                score += weights[i] * scorers[i].score(row.termFreqs[i], norm);
            }
        }
        return score;
    }
}
