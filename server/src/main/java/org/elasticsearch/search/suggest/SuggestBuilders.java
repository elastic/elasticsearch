/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

/**
 * A static factory for building suggester lookup queries
 */
public abstract class SuggestBuilders {

    /**
     * Creates a term suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder}
     * instance
     */
    public static TermSuggestionBuilder termSuggestion(String fieldname) {
        return new TermSuggestionBuilder(fieldname);
    }

    /**
     * Creates a phrase suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder}
     * instance
     */
    public static PhraseSuggestionBuilder phraseSuggestion(String fieldname) {
        return new PhraseSuggestionBuilder(fieldname);
    }

    /**
     * Creates a completion suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder}
     * instance
     */
    public static CompletionSuggestionBuilder completionSuggestion(String fieldname) {
        return new CompletionSuggestionBuilder(fieldname);
    }
}
