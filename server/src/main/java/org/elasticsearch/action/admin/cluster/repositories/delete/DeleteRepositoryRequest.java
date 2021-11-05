/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Unregister repository request.
 * <p>
 * The unregister repository command just unregisters the repository. No data is getting deleted from the repository.
 */
public class DeleteRepositoryRequest extends AcknowledgedRequest<DeleteRepositoryRequest> {

    private String name;

    public DeleteRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
    }

    public DeleteRepositoryRequest() {}

    /**
     * Constructs a new unregister repository request with the provided name.
     *
     * @param name name of the repository
     */
    public DeleteRepositoryRequest(String name) {
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
    public DeleteRepositoryRequest name(String name) {
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
