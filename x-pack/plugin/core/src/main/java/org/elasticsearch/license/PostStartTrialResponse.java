/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

class PostStartTrialResponse extends ActionResponse {

    enum Status {
        UPGRADED_TO_TRIAL(true, null, RestStatus.OK),
        TRIAL_ALREADY_ACTIVATED(false, "Operation failed: Trial was already activated.", RestStatus.FORBIDDEN);

        private final boolean isTrialStarted;
        private final String errorMessage;
        private final RestStatus restStatus;

        Status(boolean isTrialStarted, String errorMessage, RestStatus restStatus) {
            this.isTrialStarted = isTrialStarted;
            this.errorMessage = errorMessage;
            this.restStatus = restStatus;
        }

        boolean isTrialStarted() {
            return isTrialStarted;
        }

        String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }
    }

    private Status status;

    PostStartTrialResponse() {
    }

    PostStartTrialResponse(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        status = in.readEnum(Status.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
    }
}
