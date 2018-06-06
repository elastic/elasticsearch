/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class BulkShardOperationsRequestBuilder
        extends ActionRequestBuilder<BulkShardOperationsRequest, BulkShardOperationsResponse> {

    public BulkShardOperationsRequestBuilder(final ElasticsearchClient client) {
        super(client, BulkShardOperationsAction.INSTANCE, new BulkShardOperationsRequest());
    }

}
