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

public class DeletePipelineRequestBuilder extends ActionRequestBuilder<DeletePipelineRequest, AcknowledgedResponse> {

    public DeletePipelineRequestBuilder(ElasticsearchClient client, DeletePipelineAction action) {
        super(client, action, new DeletePipelineRequest());
    }

    public DeletePipelineRequestBuilder(ElasticsearchClient client, DeletePipelineAction action, String id) {
        super(client, action, new DeletePipelineRequest(id));
    }

    /**
     * Sets the id of the pipeline to delete.
     */
    public DeletePipelineRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

}
