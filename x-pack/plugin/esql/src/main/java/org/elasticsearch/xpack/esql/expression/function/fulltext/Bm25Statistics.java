/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The statistics of one BM25 statistics group, as the statistics pass computed them: N (rows with at least one
 * token), the summed length (all tokens of those rows) and the df of each query term.
 * <p>
 * The values are kept as computed. {@link SuppliedStatisticsSearcher} clamps them when Lucene asks, because only a
 * scored row can show that the data changed between the passes.
 */
final class Bm25Statistics {

    private final long docCount;
    private final long summedLength;
    private final Map<BytesRef, Long> docFreqs;
    private final boolean empty;

    private Bm25Statistics(long docCount, long summedLength, Map<BytesRef, Long> docFreqs, boolean empty) {
        this.docCount = docCount;
        this.summedLength = summedLength;
        this.docFreqs = docFreqs;
        this.empty = empty;
    }

    /**
     * @param terms  the group's sorted term list
     * @param values the k + 2 statistics in the order {@link RuntimeBm25RowStats} emits them: N, the summed length,
     *               then df for each term. A null value means the statistics are unknown, for example because
     *               {@code SUM} saw no rows or overflowed.
     */
    static Bm25Statistics of(List<BytesRef> terms, Long[] values) {
        if (values.length != terms.size() + 2) {
            throw new IllegalArgumentException("expected [" + (terms.size() + 2) + "] statistics but got [" + values.length + "]");
        }
        boolean anyNull = false;
        for (Long value : values) {
            anyNull |= value == null;
        }
        if (anyNull) {
            return new Bm25Statistics(0, 0, Map.of(), true);
        }
        Map<BytesRef, Long> docFreqs = new LinkedHashMap<>(terms.size());
        for (int i = 0; i < terms.size(); i++) {
            docFreqs.put(BytesRef.deepCopyOf(terms.get(i)), values[i + 2]);
        }
        return new Bm25Statistics(values[0], values[1], Collections.unmodifiableMap(docFreqs), values[0] <= 0);
    }

    /** True when there is nothing to score against: N is 0, or a statistic is unknown. */
    boolean isEmpty() {
        return empty;
    }

    long docCount() {
        return docCount;
    }

    long summedLength() {
        return summedLength;
    }

    /** The df of {@code term}, or null if the term is not in the group's term list. */
    Long docFreq(BytesRef term) {
        return docFreqs.get(term);
    }

    /** The PROFILE text: N, the summed length, avgdl and the df of each term. */
    @Override
    public String toString() {
        if (empty) {
            return "statistics=[empty]";
        }
        String dfs = docFreqs.entrySet()
            .stream()
            .map(e -> e.getKey().utf8ToString() + "=" + e.getValue())
            .collect(Collectors.joining(", ", "{", "}"));
        return "N=" + docCount + ", summed_length=" + summedLength + ", avgdl=" + ((double) summedLength / docCount) + ", df=" + dfs;
    }
}
