/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class RRFReranker implements Reranker {

    protected final int k;

    public RRFReranker(int k) {
        this.k = k;
    }

    @Override
    public SortedTopDocs rerank(List<SortedTopDocs> sortedTopDocs) {
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        sortedTopDocs.forEach(std -> Arrays.stream(std.scoreDocs()).forEach(sd -> {sd.score = 0.0f; scoreDocs.add(sd);}));

        for (SortedTopDocs std : sortedTopDocs) {
            int rank = 0;
            for (ScoreDoc scoreDoc : std.scoreDocs()) {
                ++rank;
                scoreDoc.score += 1.0 / (k + rank);
            }
        }

        scoreDocs.sort(Comparator.comparingDouble(sd -> -sd.score));
        return new SortedTopDocs(scoreDocs.subList(0, 10).toArray(ScoreDoc[]::new), false, null, null, null, 0);
    }
}
