/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugin.noop.action.search;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;

public class NoopSearchAction extends ActionType<SearchResponse> {
    public static final NoopSearchAction INSTANCE = new NoopSearchAction();
    public static final String NAME = "mock:data/read/search";

    private NoopSearchAction() {
        super(NAME, SearchResponse::new);
    }
}
