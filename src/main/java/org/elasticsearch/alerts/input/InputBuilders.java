/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.input.simple.SimpleInput;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class InputBuilders {

    private InputBuilders() {
    }

    public static SearchInput.SourceBuilder searchInput(SearchRequest request) {
        return new SearchInput.SourceBuilder(request);
    }

    public static SearchInput.SourceBuilder searchInput(SearchRequestBuilder builder) {
        return searchInput(builder.request());
    }

    public static SimpleInput.SourceBuilder simpleInput() {
        return simpleInput(new HashMap<String, Object>());
    }

    public static SimpleInput.SourceBuilder simpleInput(Map<String, Object> data) {
        return new SimpleInput.SourceBuilder(data);
    }
}
