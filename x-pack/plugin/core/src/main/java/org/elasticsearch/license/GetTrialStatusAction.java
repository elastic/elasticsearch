/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class GetTrialStatusAction extends Action<GetTrialStatusRequest, GetTrialStatusResponse, GetTrialStatusRequestBuilder> {

    public static final GetTrialStatusAction INSTANCE = new GetTrialStatusAction();
    public static final String NAME = "cluster:admin/xpack/license/trial_status";

    private GetTrialStatusAction() {
        super(NAME);
    }

    @Override
    public GetTrialStatusRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetTrialStatusRequestBuilder(client, this);
    }

    @Override
    public GetTrialStatusResponse newResponse() {
        return new GetTrialStatusResponse();
    }
}
