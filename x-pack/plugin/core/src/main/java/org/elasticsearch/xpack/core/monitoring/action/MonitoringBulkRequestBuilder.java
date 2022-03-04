/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;

import java.io.IOException;

public class MonitoringBulkRequestBuilder extends ActionRequestBuilder<MonitoringBulkRequest, MonitoringBulkResponse> {

    public MonitoringBulkRequestBuilder(ElasticsearchClient client) {
        super(client, MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest());
    }

    public MonitoringBulkRequestBuilder add(MonitoringBulkDoc doc) {
        request.add(doc);
        return this;
    }

    public MonitoringBulkRequestBuilder add(
        final MonitoredSystem system,
        final BytesReference content,
        final XContentType xContentType,
        final long timestamp,
        final long intervalMillis
    ) throws IOException {
        request.add(system, content, xContentType, timestamp, intervalMillis);
        return this;
    }

}
