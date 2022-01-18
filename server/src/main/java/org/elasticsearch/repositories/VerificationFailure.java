/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

public class VerificationFailure {

    private String nodeId;

    private Exception cause;

    VerificationFailure(String nodeId, Exception cause) {
        this.nodeId = nodeId;
        this.cause = cause;
    }

    @Override
    public String toString() {
        return "[" + nodeId + ", '" + cause + "']";
    }

    public Exception getCause() {
        return cause;
    }
}
