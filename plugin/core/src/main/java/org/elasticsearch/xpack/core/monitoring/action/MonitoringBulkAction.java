/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class MonitoringBulkAction extends Action<MonitoringBulkRequest, MonitoringBulkResponse, MonitoringBulkRequestBuilder> {

    public static final MonitoringBulkAction INSTANCE = new MonitoringBulkAction();
    public static final String NAME = "cluster:admin/xpack/monitoring/bulk";

    private MonitoringBulkAction() {
        super(NAME);
    }

    @Override
    public MonitoringBulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new MonitoringBulkRequestBuilder(client);
    }

    @Override
    public MonitoringBulkResponse newResponse() {
        return new MonitoringBulkResponse();
    }
}

