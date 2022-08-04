/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.ActionType;

public class EntSearchAction extends ActionType<EntSearchResponse> {

    public static final EntSearchAction INSTANCE = new EntSearchAction();
    public static final String NAME = "indices:data/read/entsearch";

    private EntSearchAction() {
        super(NAME, EntSearchResponse::new);
    }
}
