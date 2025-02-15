/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get repository request
 */
public class GetRepositoriesRequest extends MasterNodeReadRequest<GetRepositoriesRequest> {

    private String[] repositories;

    /**
     * Constructs a new get repositories request with a list of repositories.
     * <p>
     * If the list of repositories is empty or it contains a single element "_all", all registered repositories
     * are returned.
     *
     * @param repositories      list of repositories
     */
    public GetRepositoriesRequest(TimeValue masterNodeTimeout, String[] repositories) {
        super(masterNodeTimeout);
        this.repositories = repositories;
    }

    public GetRepositoriesRequest(StreamInput in) throws IOException {
        super(in);
        repositories = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(repositories);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositories == null) {
            validationException = addValidationError("repositories is null", validationException);
        }
        return validationException;
    }

    /**
     * The names of the repositories.
     *
     * @return list of repositories
     */
    public String[] repositories() {
        return this.repositories;
    }

    /**
     * Sets the list or repositories.
     * <p>
     * If the list of repositories is empty or it contains a single element "_all", all registered repositories
     * are returned.
     *
     * @param repositories list of repositories
     * @return this request
     */
    public GetRepositoriesRequest repositories(String[] repositories) {
        this.repositories = repositories;
        return this;
    }
}
