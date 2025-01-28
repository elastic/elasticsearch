/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CleanupRepositoryRequest extends AcknowledgedRequest<CleanupRepositoryRequest> {

    private String repository;

    public CleanupRepositoryRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, String repository) {
        super(masterNodeTimeout, ackTimeout);
        this.repository = repository;
    }

    public static CleanupRepositoryRequest readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            return new CleanupRepositoryRequest(in);
        } else {
            return new CleanupRepositoryRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, in);
        }
    }

    private CleanupRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
    }

    public CleanupRepositoryRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, StreamInput in) throws IOException {
        super(masterNodeTimeout, ackTimeout);
        repository = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            super.writeTo(out);
        }
        out.writeString(repository);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is null", null);
        }
        return validationException;
    }

    public String name() {
        return repository;
    }

    public void name(String repository) {
        this.repository = repository;
    }
}
