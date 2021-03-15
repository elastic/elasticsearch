/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.repositories.encrypted;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class EncryptedRepositoryChangePasswordRequest extends AcknowledgedRequest<EncryptedRepositoryChangePasswordRequest> {

    String repositoryName;
    @Nullable String fromPasswordName;
    @Nullable String toPasswordName;

    public EncryptedRepositoryChangePasswordRequest() {
        super();
    }

    public EncryptedRepositoryChangePasswordRequest(StreamInput in) throws IOException {
        super(in);
        repositoryName = in.readString();
        fromPasswordName = in.readOptionalString();
        toPasswordName = in.readOptionalString();
    }

    /**
     * Sets the name of the repository to change the password of.
     *
     * @param repositoryName the name of the repository
     */
    public EncryptedRepositoryChangePasswordRequest repositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
        return this;
    }

    public String repositoryName() {
        return this.repositoryName;
    }

    /**
     * Sets the name of the password, which is stored inside the node's keystore,
     * that will be retired so that no encrypted blobs will be using it hence.
     * If not set, it defaults to the repository's current password.
     *
     * @param fromPasswordName the name of the password from the keystore
     */
    public EncryptedRepositoryChangePasswordRequest fromPasswordName(@Nullable String fromPasswordName) {
        this.fromPasswordName = fromPasswordName;
        return this;
    }

    public @Nullable String fromPasswordName() {
        return this.fromPasswordName;
    }

    /**
     * Sets the name of the password, which is stored inside the node's keystore,
     * that will be used to encrypted the blobs that are currently encrypted with the retired password
     * {@link #fromPasswordName}.
     * If not set, it defaults to the repository's current password.
     *
     * @param toPasswordName the name of the password from the keystore
     */
    public EncryptedRepositoryChangePasswordRequest toPasswordName(@Nullable String toPasswordName) {
        this.toPasswordName = toPasswordName;
        return this;
    }

    public @Nullable String toPasswordName() {
        return this.toPasswordName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositoryName == null) {
            validationException = addValidationError("repository name is missing", validationException);
        }
        if (toPasswordName == null && fromPasswordName == null) {
            validationException = addValidationError("either from-password or to-password must be set", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repositoryName);
        out.writeOptionalString(fromPasswordName);
        out.writeOptionalString(toPasswordName);
    }
}
