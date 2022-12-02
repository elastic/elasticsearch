/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Shared state for the matches highlighter
 *
 * This holds two caches, one for the query's Weight which is global across all documents,
 * and one for Matches for each query, which will be cached per document.  This avoids having
 * to regenerate the weight and matches for each field being highlighted.
 */
class MatchesHighlighterState {

    private static final Matches NO_MATCHES = new Matches() {
        @Override
        public MatchesIterator getMatches(String field) {
            return null;
        }

        @Override
        public Collection<Matches> getSubMatches() {
            return Collections.emptyList();
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.emptyIterator();
        }
    };

    private final IndexSearcher searcher;
    private final Map<Query, Weight> weightCache = new HashMap<>();
    private final Map<Query, Matches> matchesCache = new HashMap<>();

    private int currentDoc = -1;
    private int currentLeafOrd = -1;

    MatchesHighlighterState(IndexReader reader) {
        this.searcher = new IndexSearcher(reader);
        this.searcher.setQueryCache(null);  // disable caching
    }

    Matches getMatches(Query query, LeafReaderContext ctx, int doc) throws IOException {
        if (currentDoc != doc || currentLeafOrd != ctx.ord) {
            matchesCache.clear();
            currentDoc = doc;
            currentLeafOrd = ctx.ord;
        }
        Weight w = weightCache.get(query);
        if (w == null) {
            w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
            weightCache.put(query, w);
        }
        Matches m = matchesCache.get(query);
        if (m == null) {
            m = w.matches(ctx, doc);
            if (m == null) {
                m = NO_MATCHES;
            }
            matchesCache.put(query, m);
        }
        if (m == NO_MATCHES) {
            return null;
        }
        return m;
    }
}
