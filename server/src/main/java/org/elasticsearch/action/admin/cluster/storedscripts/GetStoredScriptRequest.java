/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetStoredScriptRequest extends MasterNodeReadRequest<GetStoredScriptRequest> {

    protected String id;

    GetStoredScriptRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    public GetStoredScriptRequest(TimeValue masterNodeTimeout, String id) {
        super(masterNodeTimeout);
        this.id = id;
    }

    public GetStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public GetStoredScriptRequest id(String id) {
        this.id = id;

        return this;
    }

    @Override
    public String toString() {
        return "get script [" + id + "]";
    }
}
