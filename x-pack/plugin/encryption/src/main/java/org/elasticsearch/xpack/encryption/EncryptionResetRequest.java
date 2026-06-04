/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Request payload for {@code POST /_encryption/_reset}. The {@code accept_data_loss} flag must be
 * set to {@code true} — the reset destroys the project encryption key and any cluster state
 * encrypted under it is unrecoverable.
 */
public class EncryptionResetRequest extends AcknowledgedRequest<EncryptionResetRequest> {

    private final boolean acceptDataLoss;

    public EncryptionResetRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, boolean acceptDataLoss) {
        super(masterNodeTimeout, ackTimeout);
        this.acceptDataLoss = acceptDataLoss;
    }

    public EncryptionResetRequest(StreamInput in) throws IOException {
        super(in);
        this.acceptDataLoss = in.readBoolean();
    }

    public boolean acceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(acceptDataLoss);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (acceptDataLoss == false) {
            return ValidateActions.addValidationError("accept_data_loss must be set to true", null);
        }
        return null;
    }
}
