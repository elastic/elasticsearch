/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Rescore phase of a search request, used to run potentially expensive scoring models against the top matching documents.
 */
public class RescorePhase {

    public static void execute(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs().topDocs;
        if (topDocs.scoreDocs.length == 0) {
            return;
        }
        try {
            for (RescoreContext ctx : context.rescore()) {
                topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                // It is the responsibility of the rescorer to sort the resulted top docs,
                // here we only assert that this condition is met.
                assert context.sort() == null && topDocsSortedByScore(topDocs) : "topdocs should be sorted after rescore";
            }
            context.queryResult()
                .topDocs(new TopDocsAndMaxScore(topDocs, topDocs.scoreDocs[0].score), context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new ElasticsearchException("Rescore Phase Failed", e);
        }
    }

    /**
     * Returns true if the provided docs are sorted by score.
     */
    private static boolean topDocsSortedByScore(TopDocs topDocs) {
        if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length < 2) {
            return true;
        }
        float lastScore = topDocs.scoreDocs[0].score;
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc doc = topDocs.scoreDocs[i];
            if (Float.compare(doc.score, lastScore) > 0) {
                return false;
            }
            lastScore = doc.score;
        }
        return true;
    }
}
