/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;

public class SimulatePipelineRequestBuilder extends ActionRequestBuilder<SimulatePipelineRequest, SimulatePipelineResponse> {

    /**
     * Create a new builder for {@link SimulatePipelineRequest}s
     */
    public SimulatePipelineRequestBuilder(ElasticsearchClient client, SimulatePipelineAction action) {
        super(client, action, new SimulatePipelineRequest());
    }

    /**
     * Create a new builder for {@link SimulatePipelineRequest}s
     */
    public SimulatePipelineRequestBuilder(
        ElasticsearchClient client,
        SimulatePipelineAction action,
        BytesReference source,
        XContentType xContentType
    ) {
        super(client, action, new SimulatePipelineRequest(source, xContentType));
    }

    /**
     * Set the id for the pipeline to simulate
     */
    public SimulatePipelineRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

    /**
     * Enable or disable verbose mode
     */
    public SimulatePipelineRequestBuilder setVerbose(boolean verbose) {
        request.setVerbose(verbose);
        return this;
    }

}
