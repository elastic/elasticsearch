/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class AlertsServiceRequest extends MasterNodeOperationRequest<AlertsServiceRequest> {

    private String command;

    /**
     * Starts alerting if not already started.
     */
    public void start() {
        command = "start";
    }

    /**
     * Stops alerting if not already stopped.
     */
    public void stop() {
        command = "stop";
    }

    /**
     * Starts and stops alerting.
     */
    public void restart() {
        command = "restart";
    }

    String getCommand() {
        return command;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (command == null) {
            return ValidateActions.addValidationError("no command specified", null);
        } else {
            return null;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        command = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(command);
    }
}
