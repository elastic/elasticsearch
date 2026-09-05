/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * An exception indicating that a transport action was not found.
 *
 *
 */
public class ActionNotFoundTransportException extends TransportException {

    private final String action;
    private final List<String> availableActions;

    public ActionNotFoundTransportException(StreamInput in) throws IOException {
        super(in);
        action = in.readOptionalString();
        availableActions = (List<String>) in.readOptionalStreamable();
    }

    public ActionNotFoundTransportException(String action) {
        super("No handler for action [" + action + "]");
        this.action = action;
        this.availableActions = availableActions;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(action);
        out.writeOptionalStreamable(availableActions);
    }

    public String action() {
        return this.action;
    }

    public List<String> getAvailableActions() {
        return this.availableActions;
    }

    @Override
    public String getMessage() {
        String baseMessage = super.getMessage();
        if (availableActions != null && !availableActions.isEmpty()) {
            return baseMessage + "\nValid actions are: " + String.join(", ", availableActions);
        } else {
            return baseMessage;
        }
    }   
}
