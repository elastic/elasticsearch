/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.MatchesIterator;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.List;

/**
 * A highlighter that uses the output of a query's Matches to highlight tokens
 */
public class MatchesHighlighter implements Highlighter {

    private static final String MATCHES_HIGHLIGHTER_CONFIG_KEY = "matches_highlighter_config_key";

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {

        MatchesHighlighterState state = (MatchesHighlighterState) fieldContext.cache.computeIfAbsent(
            MATCHES_HIGHLIGHTER_CONFIG_KEY,
            k -> new MatchesHighlighterState(fieldContext.context.searcher().getIndexReader())
        );

        MatchesFieldHighlighter fieldHighlighter = new MatchesFieldHighlighter(fieldContext, state);

        MatchesIterator it = fieldHighlighter.getMatchesIterator();
        if (it == null) {
            return null;
        }

        List<CharSequence> sourceValues = HighlightUtils.loadFieldValues(
            fieldContext.fieldType,
            fieldContext.context.getSearchExecutionContext(),
            fieldContext.hitContext,
            fieldContext.forceSource
        ).stream().map(v -> (CharSequence) v.toString()).toList();

        List<String> highlights = fieldHighlighter.buildHighlights(it, sourceValues);
        return new HighlightField(fieldContext.fieldName, highlights);
    }
}
