/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlStatsRequestBuilder extends NodesOperationRequestBuilder<SqlStatsRequest, SqlStatsResponse, SqlStatsRequestBuilder> {

    public SqlStatsRequestBuilder(ElasticsearchClient client) {
        super(client, SqlStatsAction.INSTANCE, new SqlStatsRequest());
    }

}
