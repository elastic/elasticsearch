/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class PostStartBasicAction extends Action<PostStartBasicRequest, PostStartBasicResponse, PostStartBasicRequestBuilder> {

    public static final PostStartBasicAction INSTANCE = new PostStartBasicAction();
    public static final String NAME = "cluster:admin/xpack/license/start_basic";

    private PostStartBasicAction() {
        super(NAME);
    }

    @Override
    public PostStartBasicRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PostStartBasicRequestBuilder(client, this);
    }

    @Override
    public PostStartBasicResponse newResponse() {
        return new PostStartBasicResponse();
    }
}
