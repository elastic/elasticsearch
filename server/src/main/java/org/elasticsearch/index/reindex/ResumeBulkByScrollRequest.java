/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ResumeBulkByScrollRequest extends ActionRequest {

    private final AbstractBulkByScrollRequest<?> delegate;

    public ResumeBulkByScrollRequest(AbstractBulkByScrollRequest<?> delegate) {
        super();
        this.delegate = Objects.requireNonNull(delegate, "delegate request cannot be null");
    }

    public ResumeBulkByScrollRequest(StreamInput in, Writeable.Reader<? extends AbstractBulkByScrollRequest<?>> delegateReader)
        throws IOException {
        super(in);
        this.delegate = delegateReader.read(in);
    }

    public AbstractBulkByScrollRequest<?> getDelegate() {
        return delegate;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = delegate.validate();
        if (delegate.getResumeInfo().isEmpty()) {
            e = addValidationError("No resume information provided", e);
        }
        if (delegate.getShouldStoreResult() == false) {
            e = addValidationError("Resumed task result should be stored", e);
        }
        if (delegate.isEligibleForRelocationOnShutdown() == false) {
            e = addValidationError("Resumed task should be eligible for relocation on shutdown", e);
        }
        return e;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeWriteable(delegate);
    }
}
