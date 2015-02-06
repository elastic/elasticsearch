/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.ack;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This class contains the ackState of the alert, if the alert was successfully acked this will be ACK
 */
public class AckAlertResponse extends ActionResponse {

    private Alert.Status status;

    public AckAlertResponse() {
    }

    public AckAlertResponse(@Nullable Alert.Status status) {
        this.status = status;
    }

    /**
     * @return The ack state for the alert
     */
    public Alert.Status getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readBoolean() ? Alert.Status.read(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(status != null);
        if (status != null) {
            status.writeTo(out);
        }
    }
}
