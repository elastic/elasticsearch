/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;

public class MonitoringBulkRequestBuilder
        extends ActionRequestBuilder<MonitoringBulkRequest, MonitoringBulkResponse, MonitoringBulkRequestBuilder> {

    public MonitoringBulkRequestBuilder(ElasticsearchClient client) {
        super(client, MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest());
    }

    public MonitoringBulkRequestBuilder add(MonitoringBulkDoc doc) {
        request.add(doc);
        return this;
    }

    public MonitoringBulkRequestBuilder add(BytesReference content, String defaultId, String defaultVersion, String defaultIndex,
                                            String defaultType) throws Exception {
        request.add(content, defaultId, defaultVersion, defaultIndex, defaultType);
        return this;
    }
}
