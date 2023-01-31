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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RRFReranker implements Reranker {

    protected final int size;
    protected final int windowSize;
    protected final int kConstant;

    public RRFReranker(int windowSize, int size, int kConstant) {
        this.windowSize = windowSize;
        this.size = size;
        this.kConstant = kConstant;
    }

    @Override
    public int windowSize() {
        return windowSize;
    }

    @Override
    public int size() {
        return size;
    }

    public int kConstant() {
        return kConstant;
    }

    @Override
    public SortedTopDocs rerank(List<SortedTopDocs> sortedTopDocs) {
        Map<String, ScoreDoc> scoreDocs = new HashMap<>();

        for (SortedTopDocs std : sortedTopDocs) {
            int rank = 0;
            for (ScoreDoc scoreDoc : std.scoreDocs()) {
                ++rank;
                final int frank = rank;
                scoreDocs.compute(scoreDoc.doc + ":" + scoreDoc.shardIndex, (key, value) -> {
                    if (value == null) {
                        value = new ScoreDoc(scoreDoc.doc, 0.0f, scoreDoc.shardIndex);
                    }

                    value.score += 1.0f / (kConstant + frank);

                    return value;
                });
            }
        }

        List<ScoreDoc> sorted = new ArrayList<>(scoreDocs.values());
        sorted.sort(Comparator.comparingDouble(sd -> -sd.score));
        return new SortedTopDocs(sorted.subList(0, Math.min(this.size, sorted.size())).toArray(ScoreDoc[]::new), false, null, null, null, 0);
    }
}
