/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.esql.EsqlStreamActionNames;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultResponse;

/**
 * Reads the stored result of an ES|QL stream, the counterpart of {@code EsqlAsyncGetResultAction}.
 */
public class EsqlStreamGetResultAction extends ActionType<GetStreamResultResponse> {

    public static final EsqlStreamGetResultAction INSTANCE = new EsqlStreamGetResultAction();

    public static final String NAME = EsqlStreamActionNames.ESQL_STREAM_GET_RESULT_ACTION_NAME;

    private EsqlStreamGetResultAction() {
        super(NAME);
    }
}
