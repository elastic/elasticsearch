/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/** A request to cancel an ongoing reindex task. */
public class CancelReindexRequest extends BaseTasksRequest<CancelReindexRequest> {

    private final boolean waitForCompletion;

    public CancelReindexRequest(StreamInput in) throws IOException {
        super(in);
        waitForCompletion = in.readBoolean();
    }

    public CancelReindexRequest(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public String getDescription() {
        return "waitForCompletion[" + waitForCompletion + "], targetTaskId[" + getTargetTaskId() + "]";
    }

    @Override
    public org.elasticsearch.action.ActionRequestValidationException validate() {
        var validationException = super.validate();
        if (getTargetTaskId().isSet() == false) {
            validationException = addValidationError("task id must be provided", validationException);
        }
        return validationException;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public boolean match(Task task) {
        throw new UnsupportedOperationException("shouldn't be called. transport overrides function which does.");
    }
}
