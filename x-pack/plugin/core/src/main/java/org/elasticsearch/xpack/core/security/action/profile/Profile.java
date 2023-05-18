/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public record Profile(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileUser user,
    Map<String, Object> labels,
    Map<String, Object> applicationData,
    VersionControl versionControl
) implements Writeable, ToXContentObject {

    public record QualifiedName(String username, String realmDomain) {}

    public record ProfileUser(
        String username,
        List<String> roles,
        String realmName,
        @Nullable String domainName,
        String email,
        String fullName
    ) implements Writeable, ToXContent {

        public ProfileUser(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readStringList(),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString()
            );
        }

        public QualifiedName qualifiedName() {
            return new QualifiedName(username, domainName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("user");
            builder.field("username", username);
            builder.field("roles", roles);
            builder.field("realm_name", realmName);
            if (domainName != null) {
                builder.field("realm_domain", domainName);
            }
            if (email != null) {
                builder.field("email", email);
            }
            if (fullName != null) {
                builder.field("full_name", fullName);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(username);
            out.writeStringCollection(roles);
            out.writeString(realmName);
            out.writeOptionalString(domainName);
            out.writeOptionalString(email);
            out.writeOptionalString(fullName);
        }
    }

    public record VersionControl(long primaryTerm, long seqNo) implements Writeable, ToXContent {

        public VersionControl(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("_doc");
            builder.field("_primary_term", primaryTerm);
            builder.field("_seq_no", seqNo);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(primaryTerm);
            out.writeLong(seqNo);
        }
    }

    public Profile(StreamInput in) throws IOException {
        this(in.readString(), in.readBoolean(), in.readLong(), new ProfileUser(in), in.readMap(), in.readMap(), new VersionControl(in));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        versionControl.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("uid", uid);
        builder.field("enabled", enabled);
        builder.field("last_synchronized", lastSynchronized);
        user.toXContent(builder, params);
        builder.field("labels", labels);
        builder.field("data", applicationData);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uid);
        out.writeBoolean(enabled);
        out.writeLong(lastSynchronized);
        user.writeTo(out);
        out.writeGenericMap(labels);
        out.writeGenericMap(applicationData);
        versionControl.writeTo(out);
    }
}
