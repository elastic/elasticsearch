/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class GetBasicStatusRequestBuilder extends ActionRequestBuilder<GetBasicStatusRequest, GetBasicStatusResponse> {

    GetBasicStatusRequestBuilder(ElasticsearchClient client) {
        super(client, GetBasicStatusAction.INSTANCE, new GetBasicStatusRequest());
    }
}
