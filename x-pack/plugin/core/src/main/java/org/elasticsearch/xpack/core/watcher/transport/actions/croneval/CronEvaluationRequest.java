/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.croneval;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;

/*
 * No serialization because the request is always answered locally and never sent over the network
 */
public class CronEvaluationRequest extends ActionRequest {

    private String expression;
    private int count = 10;

    public CronEvaluationRequest() {
    }

    public CronEvaluationRequest(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(this.expression)) {
            validationException = ValidateActions.addValidationError("a cron expression must be supplied", validationException);
        }
        if (this.count < 1) {
            validationException = ValidateActions.addValidationError("count must be positive", validationException);
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "croneval[" + expression + "]";
    }
}
