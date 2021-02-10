/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.term;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.DirectSpellcheckerSettings;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

final class TermSuggestionContext extends SuggestionContext {

    private final DirectSpellcheckerSettings settings = new DirectSpellcheckerSettings();

    TermSuggestionContext(SearchExecutionContext searchExecutionContext) {
        super(TermSuggester.INSTANCE, searchExecutionContext);
    }

    public DirectSpellcheckerSettings getDirectSpellCheckerSettings() {
        return settings;
    }

    @Override
    public String toString() {
        return "SpellcheckerSettings" + settings + ", BaseSettings" + super.toString();
    }

}
