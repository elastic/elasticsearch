/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;

public class EsqlCursorAction extends ActionType<EsqlQueryResponse> {

    public static final EsqlCursorAction INSTANCE = new EsqlCursorAction();
    public static final String NAME = "indices:data/read/esql/cursor";

    private EsqlCursorAction() {
        super(NAME);
    }
}
