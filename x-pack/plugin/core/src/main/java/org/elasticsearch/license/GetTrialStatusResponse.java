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

class GetTrialStatusResponse extends ActionResponse {

    private boolean eligibleToStartTrial;

    GetTrialStatusResponse() {
    }

    GetTrialStatusResponse(boolean eligibleToStartTrial) {
        this.eligibleToStartTrial = eligibleToStartTrial;
    }

    boolean isEligibleToStartTrial() {
        return eligibleToStartTrial;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        eligibleToStartTrial = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(eligibleToStartTrial);
    }
}
