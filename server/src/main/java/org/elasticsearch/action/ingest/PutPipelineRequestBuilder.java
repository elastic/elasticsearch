/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;

public class PutPipelineRequestBuilder extends ActionRequestBuilder<PutPipelineRequest, AcknowledgedResponse> {

    public PutPipelineRequestBuilder(ElasticsearchClient client, PutPipelineAction action) {
        super(client, action, new PutPipelineRequest());
    }

    public PutPipelineRequestBuilder(
        ElasticsearchClient client,
        PutPipelineAction action,
        String id,
        BytesReference source,
        XContentType xContentType
    ) {
        super(client, action, new PutPipelineRequest(id, source, xContentType));
    }
}
