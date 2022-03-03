/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ActivateProfileResponse extends ActionResponse implements ToXContentObject {

    private final Profile profile;

    public ActivateProfileResponse(Profile profile) {
        this.profile = profile;
    }

    public ActivateProfileResponse(StreamInput in) throws IOException {
        super(in);
        this.profile = new Profile(in);
    }

    public Profile getProfile() {
        return profile;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        profile.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        profile.toXContent(builder, params);
        return builder;
    }
}
