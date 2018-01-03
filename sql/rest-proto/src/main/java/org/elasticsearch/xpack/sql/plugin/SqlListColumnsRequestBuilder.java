/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class SqlListColumnsRequestBuilder extends
        ActionRequestBuilder<SqlListColumnsRequest, SqlListColumnsResponse, SqlListColumnsRequestBuilder> {

    public SqlListColumnsRequestBuilder(ElasticsearchClient client, SqlListColumnsAction action, 
                                        String indexPattern, String columnPattern) {
        super(client, action, new SqlListColumnsRequest(indexPattern, columnPattern));
    }

    public SqlListColumnsRequestBuilder(ElasticsearchClient client, SqlListColumnsAction action) {
        super(client, action, new SqlListColumnsRequest());
    }

    public SqlListColumnsRequestBuilder indexPattern(String indexPattern) {
        request.setTablePattern(indexPattern);
        return this;
    }

    public SqlListColumnsRequestBuilder columnPattern(String columnPattern) {
        request.setColumnPattern(columnPattern);
        return this;
    }
}
