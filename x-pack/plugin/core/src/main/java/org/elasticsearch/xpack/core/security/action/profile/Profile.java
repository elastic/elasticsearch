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
    Access access,
    Map<String, Object> applicationData,
    VersionControl versionControl
) implements Writeable, ToXContentObject {

    public record QualifiedName(String username, String realmDomain) {}

    public record ProfileUser(
        String username,
        String realmName,
        @Nullable String realmDomain,
        String email,
        String fullName,
        String displayName
    ) implements Writeable, ToXContent {

        public ProfileUser(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString()
            );
        }

        public QualifiedName qualifiedName() {
            return new QualifiedName(username, realmDomain);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("user");
            builder.field("username", username);
            builder.field("realm_name", realmName);
            if (realmDomain != null) {
                builder.field("realm_domain", realmDomain);
            }
            if (email != null) {
                builder.field("email", email);
            }
            if (fullName != null) {
                builder.field("full_name", email);
            }
            if (displayName != null) {
                builder.field("display_name", displayName);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(username);
            out.writeString(realmName);
            out.writeOptionalString(realmDomain);
            out.writeOptionalString(email);
            out.writeOptionalString(fullName);
            out.writeOptionalString(displayName);
        }
    }

    public record Access(List<String> roles, Map<String, Object> applications) implements Writeable, ToXContent {

        public Access(StreamInput in) throws IOException {
            this(in.readStringList(), in.readMap());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("access");
            builder.field("roles", roles);
            builder.field("applications", applications);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(roles);
            out.writeMap(applications);
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
        this(in.readString(), in.readBoolean(), in.readLong(), new ProfileUser(in), new Access(in), in.readMap(), new VersionControl(in));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("uid", uid);
        builder.field("enabled", enabled);
        builder.field("last_synchronized", lastSynchronized);
        user.toXContent(builder, params);
        access.toXContent(builder, params);
        builder.field("data", applicationData);
        versionControl.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uid);
        out.writeBoolean(enabled);
        out.writeLong(lastSynchronized);
        user.writeTo(out);
        access.writeTo(out);
        out.writeMap(applicationData);
        versionControl.writeTo(out);
    }
}
