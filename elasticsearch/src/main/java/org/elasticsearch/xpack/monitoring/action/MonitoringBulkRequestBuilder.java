/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class MonitoringBulkRequestBuilder
        extends ActionRequestBuilder<MonitoringBulkRequest, MonitoringBulkResponse, MonitoringBulkRequestBuilder> {

    public MonitoringBulkRequestBuilder(ElasticsearchClient client) {
        super(client, MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest());
    }

    public MonitoringBulkRequestBuilder add(MonitoringBulkDoc doc) {
        request.add(doc);
        return this;
    }

    public MonitoringBulkRequestBuilder add(BytesReference content, String defaultId, String defaultApiVersion, String defaultType,
                                            XContentType xContentType) throws IOException {
        request.add(content, defaultId, defaultApiVersion, defaultType, xContentType);
        return this;
    }

}
