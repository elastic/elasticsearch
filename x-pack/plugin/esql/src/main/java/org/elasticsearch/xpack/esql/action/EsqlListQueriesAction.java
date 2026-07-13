/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.esql.plugin.EsqlListQueriesResponse;

public class EsqlListQueriesAction extends ActionType<EsqlListQueriesResponse> {
    public static final EsqlListQueriesAction INSTANCE = new EsqlListQueriesAction();
    public static final String NAME = "cluster:monitor/xpack/esql/list_queries";

    private EsqlListQueriesAction() {
        super(NAME);
    }
}
