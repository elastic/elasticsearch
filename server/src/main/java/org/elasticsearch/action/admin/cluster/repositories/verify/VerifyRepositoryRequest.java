/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Verify repository request.
 */
public class VerifyRepositoryRequest extends AcknowledgedRequest<VerifyRepositoryRequest> {

    private String name;

    public VerifyRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
    }

    public VerifyRepositoryRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
        super(masterNodeTimeout, ackTimeout);
    }

    /**
     * Constructs a new unregister repository request with the provided name.
     *
     * @param name              name of the repository
     */
    public VerifyRepositoryRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        this(masterNodeTimeout, ackTimeout);
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the repository to unregister.
     *
     * @param name name of the repository
     */
    public VerifyRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the repository.
     *
     * @return the name of the repository
     */
    public String name() {
        return this.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
