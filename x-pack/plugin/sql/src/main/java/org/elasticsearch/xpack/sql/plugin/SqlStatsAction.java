/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionType;

public class SqlStatsAction extends ActionType<SqlStatsResponse> {

    public static final SqlStatsAction INSTANCE = new SqlStatsAction();
    public static final String NAME = "cluster:monitor/xpack/sql/stats/dist";

    private SqlStatsAction() {
        super(NAME, SqlStatsResponse::new);
    }
}
