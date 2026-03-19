/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlDeleteCursorRequest extends ActionRequest {

    private final String cursor;
    private final boolean waitForCompletion;

    public EsqlDeleteCursorRequest(String cursor) {
        this(cursor, false);
    }

    public EsqlDeleteCursorRequest(String cursor, boolean waitForCompletion) {
        this.cursor = cursor;
        this.waitForCompletion = waitForCompletion;
    }

    public EsqlDeleteCursorRequest(StreamInput in) throws IOException {
        super(in);
        this.cursor = in.readString();
        this.waitForCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cursor);
        out.writeBoolean(waitForCompletion);
    }

    public String cursor() {
        return cursor;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (cursor == null || cursor.isEmpty()) {
            validationException = addValidationError("[cursor] is required", validationException);
        }
        return validationException;
    }
}
