/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRefBuilder;

import java.io.IOException;

public abstract class Suggester<T extends SuggestionSearchContext.SuggestionContext> {

    protected abstract Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(
        String name,
        T suggestion,
        IndexSearcher searcher,
        CharsRefBuilder spare
    ) throws IOException;

    protected abstract Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> emptySuggestion(
        String name,
        T suggestion,
        CharsRefBuilder spare
    ) throws IOException;

    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> execute(
        String name,
        T suggestion,
        IndexSearcher searcher,
        CharsRefBuilder spare
    ) throws IOException {

        // we only want to output an empty suggestion on empty shards
        if (searcher.getIndexReader().numDocs() == 0) {
            return emptySuggestion(name, suggestion, spare);
        }
        return innerExecute(name, suggestion, searcher, spare);
    }

}
