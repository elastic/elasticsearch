/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.croneval;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class CronEvaluationRequestBuilder extends ActionRequestBuilder<CronEvaluationRequest, CronEvaluationResponse> {

    public CronEvaluationRequestBuilder(ElasticsearchClient client) {
        super(client, CronEvaluationAction.INSTANCE, new CronEvaluationRequest());
    }

    public CronEvaluationRequestBuilder(ElasticsearchClient client, String expression) {
        super(client, CronEvaluationAction.INSTANCE, new CronEvaluationRequest(expression));
    }

    /**
     * Sets the expression to be evaluated
     */
    public CronEvaluationRequestBuilder setExpression(String expression) {
        this.request().setExpression(expression);
        return this;
    }
}
