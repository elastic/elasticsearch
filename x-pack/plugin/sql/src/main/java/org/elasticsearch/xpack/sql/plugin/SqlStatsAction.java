/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlStatsAction extends Action<SqlStatsRequest, SqlStatsResponse, SqlStatsRequestBuilder> {

    public static final SqlStatsAction INSTANCE = new SqlStatsAction();
    public static final String NAME = "cluster:monitor/xpack/sql/stats/dist";

    private SqlStatsAction() {
        super(NAME);
    }

    @Override
    public SqlStatsResponse newResponse() {
        return new SqlStatsResponse();
    }

    @Override
    public SqlStatsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SqlStatsRequestBuilder(client);
    }
}
