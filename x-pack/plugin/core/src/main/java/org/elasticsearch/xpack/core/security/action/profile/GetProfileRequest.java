/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class GetProfileRequest extends ActionRequest {

    private final String uid;
    private final Set<String> dataKeys;

    public GetProfileRequest(String uid, Set<String> dataKeys) {
        this.uid = uid;
        this.dataKeys = dataKeys;
    }

    public GetProfileRequest(StreamInput in) throws IOException {
        super(in);
        this.uid = in.readString();
        this.dataKeys = in.readSet(StreamInput::readString);
    }

    public String getUid() {
        return uid;
    }

    public Set<String> getDataKeys() {
        return dataKeys;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uid);
        out.writeStringCollection(dataKeys);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
