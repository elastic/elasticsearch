/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SetProfileEnabledRequest extends ActionRequest {

    private final String uid;
    private final boolean enabled;
    private final WriteRequest.RefreshPolicy refreshPolicy;

    public SetProfileEnabledRequest(String uid, boolean enabled, WriteRequest.RefreshPolicy refreshPolicy) {
        super();
        this.uid = uid;
        this.enabled = enabled;
        this.refreshPolicy = refreshPolicy;

    }

    public SetProfileEnabledRequest(StreamInput in) throws IOException {
        super(in);
        this.uid = in.readString();
        this.enabled = in.readBoolean();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uid);
        out.writeBoolean(enabled);
        refreshPolicy.writeTo(out);
    }

    public String getUid() {
        return uid;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
