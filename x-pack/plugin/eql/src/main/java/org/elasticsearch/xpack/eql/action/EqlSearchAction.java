/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionType;

public class EqlSearchAction extends ActionType<EqlSearchResponse> {
    public static final EqlSearchAction INSTANCE = new EqlSearchAction();
    public static final String NAME = "indices:data/read/eql";

    private EqlSearchAction() {
        super(NAME, EqlSearchResponse::new);
    }
}
