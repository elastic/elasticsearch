/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class PostStartTrialResponse extends ActionResponse {

    enum STATUS {
        UPGRADED_TO_TRIAL,
        TRIAL_ALREADY_ACTIVATED
    }

    private STATUS status;

    PostStartTrialResponse() {
    }

    PostStartTrialResponse(STATUS status) {
        this.status = status;
    }

    public STATUS getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        status = in.readEnum(STATUS.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
    }
}
