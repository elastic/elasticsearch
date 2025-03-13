/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest;

import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Suggest phase of a search request, used to collect suggestions
 */
public class SuggestPhase {

    private SuggestPhase() {}

    public static void execute(SearchContext context) {
        final SuggestionSearchContext suggest = context.suggest();
        if (suggest == null) {
            return;
        }
        try {
            CharsRefBuilder spare = new CharsRefBuilder();
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>(suggest.suggestions().size());

            for (Map.Entry<String, SuggestionSearchContext.SuggestionContext> entry : suggest.suggestions().entrySet()) {
                SuggestionSearchContext.SuggestionContext suggestion = entry.getValue();
                Suggester<SuggestionContext> suggester = suggestion.getSuggester();
                Suggestion<? extends Entry<? extends Option>> result;
                try {
                    result = suggester.execute(entry.getKey(), suggestion, context.searcher(), spare);
                } catch (ContextIndexSearcher.TimeExceededException timeExceededException) {
                    SearchTimeoutException.handleTimeout(
                        context.request().allowPartialSearchResults(),
                        context.shardTarget(),
                        context.queryResult()
                    );
                    result = suggester.emptySuggestion(entry.getKey(), suggestion, spare);
                }
                if (result != null) {
                    assert entry.getKey().equals(result.name);
                    suggestions.add(result);
                }
            }
            context.queryResult().suggest(new Suggest(suggestions));
        } catch (IOException e) {
            throw new ElasticsearchException("I/O exception during suggest phase", e);
        }
    }
}
