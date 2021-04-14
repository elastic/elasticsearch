/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionType;

public class SearchAction extends ActionType<SearchResponse> {

    public static final SearchAction INSTANCE = new SearchAction();
    public static final String NAME = "indices:data/read/search";

    private SearchAction() {
        super(NAME, SearchResponse::new);
    }

}
