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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        expression = in.readString();
        count = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(expression);
        out.writeVInt(count);
    }
}
