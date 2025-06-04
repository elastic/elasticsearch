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
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.Arrays;
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
        if (validateSort(context.sort()) == false) {
            throw new IllegalStateException("Cannot use [sort] option in conjunction with [rescore], missing a validate?");
        }
        TopDocs topDocs = context.queryResult().topDocs().topDocs;
        if (topDocs.scoreDocs.length == 0) {
            return;
        }
        // Populate FieldDoc#score using the primary sort field (_score) to ensure compatibility with top docs rescoring
        Arrays.stream(topDocs.scoreDocs).forEach(t -> {
            if (t instanceof FieldDoc fieldDoc) {
                fieldDoc.score = (float) fieldDoc.fields[0];
            }
        });
        TopFieldGroups topGroups = null;
        TopFieldDocs topFields = null;
        if (topDocs instanceof TopFieldGroups topFieldGroups) {
            assert context.collapse() != null && validateSortFields(topFieldGroups.fields);
            topGroups = topFieldGroups;
        } else if (topDocs instanceof TopFieldDocs topFieldDocs) {
            assert validateSortFields(topFieldDocs.fields);
            topFields = topFieldDocs;
        }
        try {
            Runnable cancellationCheck = getCancellationChecks(context);
            for (RescoreContext ctx : context.rescore()) {
                ctx.setCancellationChecker(cancellationCheck);
                topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                // It is the responsibility of the rescorer to sort the resulted top docs,
                // here we only assert that this condition is met.
                assert topDocsSortedByScore(topDocs) : "topdocs should be sorted after rescore";
                ctx.setCancellationChecker(null);
            }
            /*
             * Since rescorers are building top docs with score only, we must reconstruct the {@link TopFieldGroups}
             * or {@link TopFieldDocs} using their original version before rescoring.
             */
            if (topGroups != null) {
                assert context.collapse() != null;
                topDocs = rewriteTopFieldGroups(topGroups, topDocs);
            } else if (topFields != null) {
                topDocs = rewriteTopFieldDocs(topFields, topDocs);
            }
            context.queryResult()
                .topDocs(new TopDocsAndMaxScore(topDocs, topDocs.scoreDocs[0].score), context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new ElasticsearchException("Rescore Phase Failed", e);
        } catch (ContextIndexSearcher.TimeExceededException timeExceededException) {
            SearchTimeoutException.handleTimeout(
                context.request().allowPartialSearchResults(),
                context.shardTarget(),
                context.queryResult()
            );
            // if the rescore phase times out and partial results are allowed, the returned top docs from this shard won't be rescored
        }
    }

    /**
     * Returns whether the provided {@link SortAndFormats} can be used to rescore
     * top documents.
     */
    public static boolean validateSort(SortAndFormats sortAndFormats) {
        if (sortAndFormats == null) {
            return true;
        }
        return validateSortFields(sortAndFormats.sort.getSort());
    }

    private static boolean validateSortFields(SortField[] fields) {
        if (fields[0].equals(SortField.FIELD_SCORE) == false) {
            return false;
        }
        if (fields.length == 1) {
            return true;
        }

        // The ShardDocSortField can be used as a tiebreaker because it maintains
        // the natural document ID order within the shard.
        if (fields[1] instanceof ShardDocSortField == false || fields[1].getReverse()) {
            return false;
        }
        return true;
    }

    private static TopFieldDocs rewriteTopFieldDocs(TopFieldDocs originalTopFieldDocs, TopDocs rescoredTopDocs) {
        Map<Integer, FieldDoc> docIdToFieldDoc = Maps.newMapWithExpectedSize(originalTopFieldDocs.scoreDocs.length);
        for (int i = 0; i < originalTopFieldDocs.scoreDocs.length; i++) {
            docIdToFieldDoc.put(originalTopFieldDocs.scoreDocs[i].doc, (FieldDoc) originalTopFieldDocs.scoreDocs[i]);
        }
        var newScoreDocs = new FieldDoc[rescoredTopDocs.scoreDocs.length];
        int pos = 0;
        for (var doc : rescoredTopDocs.scoreDocs) {
            newScoreDocs[pos] = docIdToFieldDoc.get(doc.doc);
            newScoreDocs[pos].score = doc.score;
            newScoreDocs[pos].fields[0] = newScoreDocs[pos].score;
            pos++;
        }
        return new TopFieldDocs(originalTopFieldDocs.totalHits, newScoreDocs, originalTopFieldDocs.fields);
    }

    private static TopFieldGroups rewriteTopFieldGroups(TopFieldGroups originalTopGroups, TopDocs rescoredTopDocs) {
        var newFieldDocs = rewriteFieldDocs((FieldDoc[]) originalTopGroups.scoreDocs, rescoredTopDocs.scoreDocs);

        Map<Integer, Object> docIdToGroupValue = Maps.newMapWithExpectedSize(originalTopGroups.scoreDocs.length);
        for (int i = 0; i < originalTopGroups.scoreDocs.length; i++) {
            docIdToGroupValue.put(originalTopGroups.scoreDocs[i].doc, originalTopGroups.groupValues[i]);
        }
        var newGroupValues = new Object[originalTopGroups.groupValues.length];
        int pos = 0;
        for (var doc : rescoredTopDocs.scoreDocs) {
            newGroupValues[pos++] = docIdToGroupValue.get(doc.doc);
        }
        return new TopFieldGroups(
            originalTopGroups.field,
            originalTopGroups.totalHits,
            newFieldDocs,
            originalTopGroups.fields,
            newGroupValues
        );
    }

    private static FieldDoc[] rewriteFieldDocs(FieldDoc[] originalTopDocs, ScoreDoc[] rescoredTopDocs) {
        Map<Integer, FieldDoc> docIdToFieldDoc = Maps.newMapWithExpectedSize(rescoredTopDocs.length);
        Arrays.stream(originalTopDocs).forEach(d -> docIdToFieldDoc.put(d.doc, d));
        var newDocs = new FieldDoc[rescoredTopDocs.length];
        int pos = 0;
        for (var doc : rescoredTopDocs) {
            newDocs[pos] = docIdToFieldDoc.get(doc.doc);
            newDocs[pos].score = doc.score;
            newDocs[pos].fields[0] = doc.score;
            pos++;
        }
        return newDocs;
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
        List<Runnable> cancellationChecks = context.getCancellationChecks();
        return () -> {
            for (var check : cancellationChecks) {
                check.run();
            }
        };
    }
}
