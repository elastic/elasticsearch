/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.esql.plugin.EsqlGetQueryResponse;

public class EsqlGetQueryAction extends ActionType<EsqlGetQueryResponse> {
    public static final EsqlGetQueryAction INSTANCE = new EsqlGetQueryAction();
    public static final String NAME = "cluster:monitor/xpack/esql/get_query";

    private EsqlGetQueryAction() {
        super(NAME);
    }
}
