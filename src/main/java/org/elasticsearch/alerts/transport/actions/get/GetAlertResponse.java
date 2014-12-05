/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The GetAlertResponse the response class wraps a GetResponse containing the alert source
 */
public class GetAlertResponse extends ActionResponse {

    private GetResponse getResponse;

    public GetAlertResponse() {

    }

    public GetAlertResponse(GetResponse getResponse) {
        this.getResponse = getResponse;
    }

    /**
     * The GetResponse containing the alert source
     * @param getResponse
     */
    public void getResponse(GetResponse getResponse) {
        this.getResponse = getResponse;
    }

    public GetResponse getResponse() {
        return this.getResponse;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            getResponse = GetResponse.readGetResponse(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(getResponse != null);
        if (getResponse != null) {
            getResponse.writeTo(out);
        }
    }
}
