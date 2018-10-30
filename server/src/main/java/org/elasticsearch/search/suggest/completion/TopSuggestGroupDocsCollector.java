/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Custom {@link TopSuggestDocsCollector} that returns top documents from the completion suggester.
 * <p>
 * TODO: this should be refactored when https://issues.apache.org/jira/browse/LUCENE-8529 is fixed.
 * Unlike the parent class, this collector uses the surface form to tie-break suggestions with identical
 * scores.
 * <p>
 * This collector groups suggestions coming from the same document but matching different contexts
 * or surface form together. When different contexts or surface forms match the same suggestion form only
 * the best one per document (sorted by weight) is kept.
 * <p>
 * This collector is also able to filter duplicate suggestion coming from different documents.
 * In order to keep this feature fast, the de-duplication of suggestions with different contexts is done
 * only on the top N*num_contexts (where N is the number of documents to return) suggestions per segment.
 * This means that skip_duplicates will visit at most N*num_contexts suggestions per segment to find unique suggestions
 * that match the input. If more than N*num_contexts suggestions are duplicated with different contexts this collector
 * will not be able to return more than one suggestion even when N is greater than 1.
 **/
class TopSuggestGroupDocsCollector extends TopSuggestDocsCollector {
    private final class SuggestScoreDocPriorityQueue extends PriorityQueue<TopSuggestDocs.SuggestScoreDoc> {
        /**
         * Creates a new priority queue of the specified size.
         */
        private SuggestScoreDocPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(TopSuggestDocs.SuggestScoreDoc a, TopSuggestDocs.SuggestScoreDoc b) {
            if (a.score == b.score) {
                // tie break by completion key
                int cmp = Lookup.CHARSEQUENCE_COMPARATOR.compare(a.key, b.key);
                // prefer smaller doc id, in case of a tie
                return cmp != 0 ? cmp > 0 : a.doc > b.doc;
            }
            return a.score < b.score;
        }

        /**
         * Returns the top N results in descending order.
         */
        public TopSuggestDocs.SuggestScoreDoc[] getResults() {
            int size = size();
            TopSuggestDocs.SuggestScoreDoc[] res = new TopSuggestDocs.SuggestScoreDoc[size];
            for (int i = size - 1; i >= 0; i--) {
                res[i] = pop();
            }
            return res;
        }
    }


    private final SuggestScoreDocPriorityQueue priorityQueue;
    private final int num;

    /** Only set if we are deduplicating hits: holds all per-segment hits until the end, when we dedup them */
    private final List<TopSuggestDocs.SuggestScoreDoc> pendingResults;

    /** Only set if we are deduplicating hits: holds all surface forms seen so far in the current segment */
    final CharArraySet seenSurfaceForms;

    /** Document base offset for the current Leaf */
    protected int docBase;

    private Map<Integer, List<CharSequence>> docContexts = new HashMap<>();

    /**
     * Sole constructor
     *
     * Collects at most <code>num</code> completions
     * with corresponding document and weight
     */
    TopSuggestGroupDocsCollector(int num, boolean skipDuplicates) {
        super(1, skipDuplicates);
        if (num <= 0) {
            throw new IllegalArgumentException("'num' must be > 0");
        }
        this.num = num;
        this.priorityQueue = new SuggestScoreDocPriorityQueue(num);
        if (skipDuplicates) {
            seenSurfaceForms = new CharArraySet(num, false);
            pendingResults = new ArrayList<>();
        } else {
            seenSurfaceForms = null;
            pendingResults = null;
        }
    }

    /**
     * Returns the contexts associated with the provided <code>doc</code>.
     */
    public List<CharSequence> getContexts(int doc) {
        return docContexts.getOrDefault(doc, Collections.emptyList());
    }

    @Override
    protected boolean doSkipDuplicates() {
        return seenSurfaceForms != null;
    }

    @Override
    public int getCountToCollect() {
        return num;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        if (seenSurfaceForms != null) {
            seenSurfaceForms.clear();
            // NOTE: this also clears the priorityQueue:
            for (TopSuggestDocs.SuggestScoreDoc hit : priorityQueue.getResults()) {
                pendingResults.add(hit);
            }
        }
    }

    @Override
    public void collect(int docID, CharSequence key, CharSequence context, float score) throws IOException {
        int globalDoc = docID + docBase;
        boolean isDuplicate = docContexts.containsKey(globalDoc);
        List<CharSequence> contexts = docContexts.computeIfAbsent(globalDoc, k -> new ArrayList<>());
        if (context != null) {
            contexts.add(context);
        }
        if (isDuplicate) {
            return;
        }
        TopSuggestDocs.SuggestScoreDoc current = new TopSuggestDocs.SuggestScoreDoc(globalDoc, key, context, score);
        if (current == priorityQueue.insertWithOverflow(current)) {
            // if the current SuggestScoreDoc has overflown from pq,
            // we can assume all of the successive collections from
            // this leaf will be overflown as well
            // TODO: reuse the overflow instance?
            throw new CollectionTerminatedException();
        }
    }

    @Override
    public TopSuggestDocs get() throws IOException {

        TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocs;

        if (seenSurfaceForms != null) {
            // NOTE: this also clears the priorityQueue:
            for (TopSuggestDocs.SuggestScoreDoc hit : priorityQueue.getResults()) {
                pendingResults.add(hit);
            }

            // Deduplicate all hits: we already dedup'd efficiently within each segment by
            // truncating the FST top paths search, but across segments there may still be dups:
            seenSurfaceForms.clear();

            // TODO: we could use a priority queue here to make cost O(N * log(num)) instead of O(N * log(N)), where N = O(num *
            // numSegments), but typically numSegments is smallish and num is smallish so this won't matter much in practice:

            Collections.sort(pendingResults,
                (a, b) -> {
                    // sort by higher score
                    int cmp = Float.compare(b.score, a.score);
                    if (cmp == 0) {
                        // tie break by completion key
                        cmp = Lookup.CHARSEQUENCE_COMPARATOR.compare(a.key, b.key);
                        if (cmp == 0) {
                            // prefer smaller doc id, in case of a tie
                            cmp = Integer.compare(a.doc, b.doc);
                        }
                    }
                    return cmp;
                });

            List<TopSuggestDocs.SuggestScoreDoc> hits = new ArrayList<>();

            for (TopSuggestDocs.SuggestScoreDoc hit : pendingResults) {
                if (seenSurfaceForms.contains(hit.key) == false) {
                    seenSurfaceForms.add(hit.key);
                    hits.add(hit);
                    if (hits.size() == num) {
                        break;
                    }
                }
            }
            suggestScoreDocs = hits.toArray(new TopSuggestDocs.SuggestScoreDoc[0]);
        } else {
            suggestScoreDocs = priorityQueue.getResults();
        }

        if (suggestScoreDocs.length > 0) {
            return new TopSuggestDocs(new TotalHits(suggestScoreDocs.length, TotalHits.Relation.EQUAL_TO), suggestScoreDocs);
        } else {
            return TopSuggestDocs.EMPTY;
        }
    }

}
