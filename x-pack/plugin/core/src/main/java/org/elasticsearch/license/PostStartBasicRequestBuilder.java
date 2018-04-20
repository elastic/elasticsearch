/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class PostStartBasicRequestBuilder extends ActionRequestBuilder<PostStartBasicRequest,
        PostStartBasicResponse, PostStartBasicRequestBuilder> {

    PostStartBasicRequestBuilder(ElasticsearchClient client, PostStartBasicAction action) {
        super(client, action, new PostStartBasicRequest());
    }

    public PostStartBasicRequestBuilder setAcknowledge(boolean acknowledge) {
        request.acknowledge(acknowledge);
        return this;
    }
}
