/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdateProfileUserRequest extends ActionRequest {

    private final String uid;
    private final String email;
    private final String fullName;
    private final String displayName;
    private final Boolean active;
    private final long ifPrimaryTerm;
    private final long ifSeqNo;
    private final RefreshPolicy refreshPolicy;

    public UpdateProfileUserRequest(
        String uid,
        @Nullable String email,
        @Nullable String fullName,
        @Nullable String displayName,
        @Nullable Boolean active,
        long ifPrimaryTerm,
        long ifSeqNo,
        RefreshPolicy refreshPolicy
    ) {
        this.uid = Objects.requireNonNull(uid, "profile uid must not be null");
        this.email = email;
        this.fullName = fullName;
        this.displayName = displayName;
        this.active = active;
        this.ifPrimaryTerm = ifPrimaryTerm;
        this.ifSeqNo = ifSeqNo;
        this.refreshPolicy = refreshPolicy;
    }

    public UpdateProfileUserRequest(StreamInput in) throws IOException {
        super(in);
        this.uid = in.readString();
        this.email = in.readOptionalString();
        this.fullName = in.readOptionalString();
        this.displayName = in.readOptionalString();
        this.active = in.readOptionalBoolean();
        this.ifPrimaryTerm = in.readLong();
        this.ifSeqNo = in.readLong();
        this.refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public String getUid() {
        return uid;
    }

    public String getEmail() {
        return email;
    }

    public String getFullName() {
        return fullName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public Boolean getActive() {
        return active;
    }

    public long getIfPrimaryTerm() {
        return ifPrimaryTerm;
    }

    public long getIfSeqNo() {
        return ifSeqNo;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (email == null && fullName == null && displayName == null && active == null) {
            validationException = addValidationError("must update at least one field", validationException);
        }
        return validationException;
    }
}
