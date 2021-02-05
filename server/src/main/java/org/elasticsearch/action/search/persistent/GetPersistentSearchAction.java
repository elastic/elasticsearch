/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.search.persistent.PersistentSearchResponse;

public class GetPersistentSearchAction extends ActionType<PersistentSearchResponse> {
    public static final String NAME = "indices:data/read/persistent_search/get";
    public static final GetPersistentSearchAction INSTANCE = new GetPersistentSearchAction();

    public GetPersistentSearchAction() {
        super(NAME, PersistentSearchResponse::new);
    }
}
