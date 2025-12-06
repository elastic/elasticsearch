/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Comparator;

/**
 * A query rescorer interface used to re-rank the Top-K results of a previously
 * executed search.
 *
 * Subclasses should borrow heavily from {@link QueryRescorer} because it is
 * fairly well behaved and documents that tradeoffs that it is making. There
 * is also an {@code ExampleRescorer} that is worth looking at.
 */
public interface Rescorer {
    /**
     * Modifies the result of the previously executed search ({@link TopDocs})
     * in place based on the given {@link RescoreContext}.
     *
     * @param topDocs        the result of the previously executed search
     * @param searcher       the searcher used for this search. This will never be <code>null</code>.
     * @param rescoreContext the {@link RescoreContext}. This will never be <code>null</code>
     * @throws IOException if an {@link IOException} occurs during rescoring
     */
    TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException;

    /**
     * Executes an {@link Explanation} phase on the rescorer.
     *
     * @param topLevelDocId the global / top-level document ID to explain
     * @param searcher the searcher used for this search. This will never be <code>null</code>.
     * @param rescoreContext context for this rescorer
     * @param sourceExplanation explanation of the source of the documents being fed into this rescore
     * @return the explain for the given top level document ID.
     * @throws IOException if an {@link IOException} occurs
     */
    Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext, Explanation sourceExplanation)
        throws IOException;

    Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = (o1, o2) -> {
        int cmp = Float.compare(o2.score, o1.score);
        return cmp == 0 ? Integer.compare(o1.doc, o2.doc) : cmp;
    };

    /**
     * Returns a new {@link TopDocs} with the topN from the incoming one,
     * or the same TopDocs if the number of hits is already &lt;= topN.
     */
    static TopDocs topN(TopDocs in, int topN) {
        if (in.scoreDocs.length <= topN) {
            return in;
        }
        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);
        return new TopDocs(in.totalHits, subset);
    }

}
