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
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetProfilesResponse extends ActionResponse implements ToXContentObject {

    private final List<Profile> profiles;
    private final Map<String, Exception> errors;

    public GetProfilesResponse(List<Profile> profiles, Map<String, Exception> errors) {
        this.profiles = Objects.requireNonNull(profiles);
        this.errors = Objects.requireNonNull(errors);
    }

    public GetProfilesResponse(StreamInput in) throws IOException {
        super(in);
        this.profiles = in.readImmutableList(Profile::new);
        this.errors = in.readMap(StreamInput::readString, StreamInput::readException);
    }

    public List<Profile> getProfiles() {
        return profiles;
    }

    public Map<String, Exception> getErrors() {
        return errors;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(profiles);
        out.writeMap(errors, StreamOutput::writeString, StreamOutput::writeException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("profiles");
            for (Profile profile : profiles) {
                profile.toXContent(builder, params);
            }
            builder.endArray();
            XContentUtils.maybeAddErrorDetails(builder, errors);
        }
        builder.endObject();
        return builder;
    }
}
