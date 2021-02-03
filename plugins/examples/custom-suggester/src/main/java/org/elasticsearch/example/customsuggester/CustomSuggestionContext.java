/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.customsuggester;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.Map;

public class CustomSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    public Map<String, Object> options;

    public CustomSuggestionContext(SearchExecutionContext context, Map<String, Object> options) {
        super(new CustomSuggester(), context);
        this.options = options;
    }
}
