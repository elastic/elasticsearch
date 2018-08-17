/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

public class MoveToStepActionRequestBuilder
        extends ActionRequestBuilder<MoveToStepAction.Request, MoveToStepAction.Response, MoveToStepActionRequestBuilder> {

    public MoveToStepActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<MoveToStepAction.Request, MoveToStepAction.Response, MoveToStepActionRequestBuilder> action) {
        super(client, action, new MoveToStepAction.Request());
    }

    public MoveToStepActionRequestBuilder setIndex(final String index) {
        request.setIndex(index);
        return this;
    }

    public MoveToStepActionRequestBuilder setCurrentStepKey(final Step.StepKey currentStepKey) {
        request.setCurrentStepKey(currentStepKey);
        return this;
    }

    public MoveToStepActionRequestBuilder setNextStepKey(final Step.StepKey nextStepKey) {
        request.setNextStepKey(nextStepKey);
        return this;
    }

}
