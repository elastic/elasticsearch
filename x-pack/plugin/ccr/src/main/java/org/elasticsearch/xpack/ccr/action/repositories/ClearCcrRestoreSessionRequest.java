/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClearCcrRestoreSessionRequest extends ActionRequest {

    private String sessionUUID;

    ClearCcrRestoreSessionRequest(StreamInput in) throws IOException {
        super.readFrom(in);
        sessionUUID = in.readString();
    }

    public ClearCcrRestoreSessionRequest(String sessionUUID) {
        this.sessionUUID = sessionUUID;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sessionUUID = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
    }

    public String getSessionUUID() {
        return sessionUUID;
    }
}
