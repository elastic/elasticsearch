/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class PutOperationModeRequest extends AcknowledgedRequest<PutOperationModeRequest> {

    private OperationMode mode;

    public PutOperationModeRequest(OperationMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("mode cannot be null");
        }
        if (mode == OperationMode.STOPPED) {
            throw new IllegalArgumentException("cannot directly stop index-lifecycle");
        }
        this.mode = mode;
    }

    public PutOperationModeRequest() {
    }

    public OperationMode getMode() {
        return mode;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        mode = in.readEnum(OperationMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(mode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        PutOperationModeRequest other = (PutOperationModeRequest) obj;
        return Objects.equals(mode, other.mode);
    }
}