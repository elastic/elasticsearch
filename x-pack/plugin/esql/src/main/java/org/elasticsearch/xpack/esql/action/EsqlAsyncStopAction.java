/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.esql.EsqlAsyncActionNames;

public class EsqlAsyncStopAction extends ActionType<EsqlQueryResponse> {

    public static final EsqlAsyncStopAction INSTANCE = new EsqlAsyncStopAction();

    public static final String NAME = EsqlAsyncActionNames.ESQL_ASYNC_STOP_ACTION_NAME;

    private EsqlAsyncStopAction() {
        super(NAME);
    }
}
