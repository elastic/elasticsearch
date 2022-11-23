/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

class PostStartTrialRequestBuilder extends ActionRequestBuilder<PostStartTrialRequest, PostStartTrialResponse> {

    PostStartTrialRequestBuilder(ElasticsearchClient client) {
        super(client, PostStartTrialAction.INSTANCE, new PostStartTrialRequest());
    }

    public PostStartTrialRequestBuilder setAcknowledge(boolean acknowledge) {
        request.acknowledge(acknowledge);
        return this;
    }
}
