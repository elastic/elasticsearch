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
import org.elasticsearch.xpack.core.indexlifecycle.StartILMRequest;

public class StartILMActionRequestBuilder
        extends ActionRequestBuilder<StartILMRequest, AcknowledgedResponse, StartILMActionRequestBuilder> {

    public StartILMActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<StartILMRequest, AcknowledgedResponse, StartILMActionRequestBuilder> action) {
        super(client, action, new StartILMRequest());
    }

}
