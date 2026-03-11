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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlCursorRequest extends ActionRequest {

    private final String cursor;
    @Nullable
    private final TimeValue keepAlive;

    public EsqlCursorRequest(String cursor, @Nullable TimeValue keepAlive) {
        this.cursor = cursor;
        this.keepAlive = keepAlive;
    }

    public EsqlCursorRequest(StreamInput in) throws IOException {
        super(in);
        this.cursor = in.readString();
        this.keepAlive = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cursor);
        out.writeOptionalTimeValue(keepAlive);
    }

    public String cursor() {
        return cursor;
    }

    @Nullable
    public TimeValue keepAlive() {
        return keepAlive;
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
