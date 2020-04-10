/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class GetTrialStatusRequestBuilder extends ActionRequestBuilder<GetTrialStatusRequest, GetTrialStatusResponse> {

    GetTrialStatusRequestBuilder(ElasticsearchClient client) {
        super(client, GetTrialStatusAction.INSTANCE, new GetTrialStatusRequest());
    }
}
