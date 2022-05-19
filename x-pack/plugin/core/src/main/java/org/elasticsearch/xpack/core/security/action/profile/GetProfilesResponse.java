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
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetProfilesResponse extends ActionResponse implements StatusToXContentObject {

    private final Profile[] profiles;

    public GetProfilesResponse(@Nullable Profile profile) {
        this.profiles = profile != null ? new Profile[] { profile } : new Profile[0];
    }

    public GetProfilesResponse(StreamInput in) throws IOException {
        super(in);
        this.profiles = in.readArray(Profile::new, Profile[]::new);
    }

    public Profile[] getProfiles() {
        return profiles;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(profiles);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Profile profile : profiles) {
            builder.field(profile.uid());
            profile.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        return profiles.length > 0 ? RestStatus.OK : RestStatus.NOT_FOUND;
    }
}
