/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;

public class QuerySearchApplicationAction extends ActionType<SearchResponse> {

    public static final QuerySearchApplicationAction INSTANCE = new QuerySearchApplicationAction();
    public static final String NAME = "indices:data/read/xpack/application/search_application/search";

    public QuerySearchApplicationAction() {
        super(NAME, SearchResponse::new);
    }

}
