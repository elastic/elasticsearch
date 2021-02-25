/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * An exception indicating that a transport action was not found.
 *
 *
 */
public class ActionNotFoundTransportException extends TransportException {

    private final String action;

    public ActionNotFoundTransportException(StreamInput in) throws IOException {
        super(in);
        action = in.readOptionalString();
    }

    public ActionNotFoundTransportException(String action) {
        super("No handler for action [" + action + "]");
        this.action = action;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(action);
    }

    public String action() {
        return this.action;
    }
}
