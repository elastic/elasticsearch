/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionType;

public class EsqlStatsAction extends ActionType<EsqlStatsResponse> {

    public static final EsqlStatsAction INSTANCE = new EsqlStatsAction();
    public static final String NAME = "cluster:monitor/xpack/esql/stats/dist";

    private EsqlStatsAction() {
        super(NAME, EsqlStatsResponse::new);
    }
}
