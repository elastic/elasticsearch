/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.StopILMRequest;

public class StopILMActionRequestBuilder extends ActionRequestBuilder<StopILMRequest, AcknowledgedResponse, StopILMActionRequestBuilder> {

    public StopILMActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<StopILMRequest, AcknowledgedResponse, StopILMActionRequestBuilder> action) {
        super(client, action, new StopILMRequest());
    }

}
