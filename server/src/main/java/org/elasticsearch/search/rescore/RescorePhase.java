/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.SearchTimeoutException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Rescore phase of a search request, used to run potentially expensive scoring models against the top matching documents.
 */
public class RescorePhase {

    private RescorePhase() {}

    public static void execute(SearchContext context) {
        if (context.size() == 0 || context.rescore() == null || context.rescore().isEmpty()) {
            return;
        }

        TopDocs topDocs = context.queryResult().topDocs().topDocs;
        if (topDocs.scoreDocs.length == 0) {
            return;
        }
        TopFieldGroups topGroups = null;
        if (topDocs instanceof TopFieldGroups topFieldGroups) {
            assert context.collapse() != null;
            topGroups = topFieldGroups;
        }
        try {
            Runnable cancellationCheck = getCancellationChecks(context);
            for (RescoreContext ctx : context.rescore()) {
                ctx.setCancellationChecker(cancellationCheck);
                topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                // It is the responsibility of the rescorer to sort the resulted top docs,
                // here we only assert that this condition is met.
                assert context.sort() == null && topDocsSortedByScore(topDocs) : "topdocs should be sorted after rescore";
                ctx.setCancellationChecker(null);
            }
            if (topGroups != null) {
                assert context.collapse() != null;
                /**
                 * Since rescorers don't preserve collapsing, we must reconstruct the group and field
                 * values from the originalTopGroups to create a new {@link TopFieldGroups} from the
                 * rescored top documents.
                 */
                topDocs = rewriteTopGroups(topGroups, topDocs);
            }
            context.queryResult()
                .topDocs(new TopDocsAndMaxScore(topDocs, topDocs.scoreDocs[0].score), context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new ElasticsearchException("Rescore Phase Failed", e);
        } catch (ContextIndexSearcher.TimeExceededException e) {
            SearchTimeoutException.handleTimeout(
                context.request().allowPartialSearchResults(),
                context.shardTarget(),
                context.queryResult()
            );
        }
    }

    private static TopFieldGroups rewriteTopGroups(TopFieldGroups originalTopGroups, TopDocs rescoredTopDocs) {
        assert originalTopGroups.fields.length == 1 && SortField.FIELD_SCORE.equals(originalTopGroups.fields[0])
            : "rescore must always sort by score descending";
        Map<Integer, Object> docIdToGroupValue = Maps.newMapWithExpectedSize(originalTopGroups.scoreDocs.length);
        for (int i = 0; i < originalTopGroups.scoreDocs.length; i++) {
            docIdToGroupValue.put(originalTopGroups.scoreDocs[i].doc, originalTopGroups.groupValues[i]);
        }
        var newScoreDocs = new FieldDoc[rescoredTopDocs.scoreDocs.length];
        var newGroupValues = new Object[originalTopGroups.groupValues.length];
        int pos = 0;
        for (var doc : rescoredTopDocs.scoreDocs) {
            newScoreDocs[pos] = new FieldDoc(doc.doc, doc.score, new Object[] { doc.score });
            newGroupValues[pos++] = docIdToGroupValue.get(doc.doc);
        }
        return new TopFieldGroups(
            originalTopGroups.field,
            originalTopGroups.totalHits,
            newScoreDocs,
            originalTopGroups.fields,
            newGroupValues
        );
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

    static Runnable getCancellationChecks(SearchContext context) {
        List<Runnable> cancellationChecks = new ArrayList<>();
        if (context.lowLevelCancellation()) {
            cancellationChecks.add(() -> {
                final SearchShardTask task = context.getTask();
                if (task != null) {
                    task.ensureNotCancelled();
                }
            });
        }

        final Runnable timeoutRunnable = QueryPhase.getTimeoutCheck(context);
        if (timeoutRunnable != null) {
            cancellationChecks.add(timeoutRunnable);
        }

        return () -> {
            for (var check : cancellationChecks) {
                check.run();
            }
        };
    }
}
