/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionType;

public class SearchTemplateAction extends ActionType<SearchTemplateResponse> {

    public static final SearchTemplateAction INSTANCE = new SearchTemplateAction();
    public static final String NAME = "indices:data/read/search/template";

    private SearchTemplateAction() {
        super(NAME, SearchTemplateResponse::new);
    }
}
