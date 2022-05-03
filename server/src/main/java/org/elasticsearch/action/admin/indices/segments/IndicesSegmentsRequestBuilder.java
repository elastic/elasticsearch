/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class IndicesSegmentsRequestBuilder extends BroadcastOperationRequestBuilder<
    IndicesSegmentsRequest,
    IndicesSegmentResponse,
    IndicesSegmentsRequestBuilder> {

    public IndicesSegmentsRequestBuilder(ElasticsearchClient client, IndicesSegmentsAction action) {
        super(client, action, new IndicesSegmentsRequest());
    }
}
