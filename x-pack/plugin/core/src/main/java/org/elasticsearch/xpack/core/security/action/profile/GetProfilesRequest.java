/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetProfilesRequest extends LegacyActionRequest {

    private final List<String> uids;
    private final Set<String> dataKeys;

    public GetProfilesRequest(List<String> uids, Set<String> dataKeys) {
        this.uids = Objects.requireNonNull(uids, "profile UIDs cannot be null");
        this.dataKeys = Objects.requireNonNull(dataKeys, "data keys cannot be null");
    }

    public GetProfilesRequest(StreamInput in) throws IOException {
        super(in);
        this.uids = in.readStringCollectionAsList();
        this.dataKeys = in.readCollectionAsSet(StreamInput::readString);
    }

    public GetProfilesRequest(String uid, Set<String> dataKeys) {
        this(List.of(Objects.requireNonNull(uid, "profile UID cannot be null")), dataKeys);
    }

    public List<String> getUids() {
        return uids;
    }

    public Set<String> getDataKeys() {
        return dataKeys;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(uids);
        out.writeStringCollection(dataKeys);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (uids.isEmpty()) {
            validationException = addValidationError("profile UIDs must be provided", validationException);
        }
        if (uids.stream().anyMatch(uid -> false == Strings.hasText(uid))) {
            validationException = addValidationError("Profile UID cannot be empty", validationException);
        }
        return validationException;
    }
}
