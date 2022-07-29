/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.customsuggester;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class CustomSuggesterPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SearchPlugin.SuggesterSpec<?>> getSuggesters() {
        return Collections.singletonList(
            new SearchPlugin.SuggesterSpec<>(
                CustomSuggestionBuilder.SUGGESTION_NAME,
                CustomSuggestionBuilder::new,
                CustomSuggestionBuilder::fromXContent,
                CustomSuggestion::new
            )
        );
    }
}
