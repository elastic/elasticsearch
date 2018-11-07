/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.StopILMRequest;

public class StopILMAction extends Action<StopILMRequest, AcknowledgedResponse, StopILMActionRequestBuilder> {
    public static final StopILMAction INSTANCE = new StopILMAction();
    public static final String NAME = "cluster:admin/ilm/stop";

    protected StopILMAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    public StopILMActionRequestBuilder newRequestBuilder(final ElasticsearchClient client) {
        return new StopILMActionRequestBuilder(client, INSTANCE);
    }

}
