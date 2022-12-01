/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

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

public class MatchesHighlighterState {

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

    private final FieldHighlightContext context;
    private final IndexSearcher searcher;
    private final Map<Query, Weight> weightCache = new HashMap<>();
    private final Map<Query, Matches> matchesCache = new HashMap<>();

    private int currentDoc = -1;

    public MatchesHighlighterState(FieldHighlightContext context) {
        this.context = context;
        this.searcher = context.context.searcher();
    }

    public Matches getMatches(Query query, int doc) throws IOException {
        if (currentDoc != doc) {
            matchesCache.clear();
            currentDoc = doc;
        }
        Weight w = weightCache.get(query);
        if (w == null) {
            w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
            weightCache.put(query, w);
        }
        Matches m = matchesCache.get(query);
        if (m == null) {
            m = w.matches(context.hitContext.readerContext(), doc);
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

    public MatchesFieldHighlighter getMatchesFieldHighlighter(FieldHighlightContext fieldContext) throws IOException {
        return new MatchesFieldHighlighter(fieldContext, this);
    }
}
