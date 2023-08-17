/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Response containing a User retrieved from the security index
 */
public class GetUsersResponse extends ActionResponse implements ToXContentObject {

    private final User[] users;
    @Nullable
    private final Map<String, String> profileUidLookup;

    public GetUsersResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        if (size < 0) {
            users = null;
        } else {
            users = new User[size];
            for (int i = 0; i < size; i++) {
                final User user = Authentication.AuthenticationSerializationHelper.readUserFrom(in);
                assert false == user instanceof InternalUser : "should not get internal user [" + user + "]";
                users[i] = user;
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
            if (in.readBoolean()) {
                profileUidLookup = in.readMap(StreamInput::readString);
            } else {
                profileUidLookup = null;
            }
        } else {
            profileUidLookup = null;
        }
    }

    public GetUsersResponse(Collection<User> users) {
        this(users, null);
    }

    public GetUsersResponse(Collection<User> users, @Nullable Map<String, String> profileUidLookup) {
        this.users = users.toArray(User[]::new);
        this.profileUidLookup = profileUidLookup;
    }

    public User[] users() {
        return users;
    }

    public Map<String, String> getProfileUidLookup() {
        return profileUidLookup;
    }

    public boolean hasUsers() {
        return users != null && users.length > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(users == null ? -1 : users.length);
        if (users != null) {
            for (User user : users) {
                Authentication.AuthenticationSerializationHelper.writeUserTo(user, out);
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
            if (profileUidLookup != null) {
                out.writeBoolean(true);
                out.writeMap(profileUidLookup, StreamOutput::writeString, StreamOutput::writeString);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (User user : users) {
            builder.field(user.principal());
            builder.startObject();
            {
                user.innerToXContent(builder);
                if (profileUidLookup != null) {
                    final String profileUid = profileUidLookup.get(user.principal());
                    if (profileUid != null) {
                        builder.field("profile_uid", profileUid);
                    }
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
