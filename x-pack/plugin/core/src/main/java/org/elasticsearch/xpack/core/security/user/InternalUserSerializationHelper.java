/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class InternalUserSerializationHelper {
    public static User readFrom(StreamInput input) throws IOException {
        final boolean isInternalUser = input.readBoolean();
        final String username = input.readString();
        if (isInternalUser) {
            if (SystemUser.is(username)) {
                return SystemUser.INSTANCE;
            } else if (XPackUser.is(username)) {
                return XPackUser.INSTANCE;
            } else if (XPackSecurityUser.is(username)) {
                return XPackSecurityUser.INSTANCE;
            } else if (AsyncSearchUser.is(username)) {
                return AsyncSearchUser.INSTANCE;
            }
            throw new IllegalStateException("user [" + username + "] is not an internal user");
        }
        return User.partialReadFrom(username, input);
    }
    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (SystemUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(SystemUser.NAME);
        } else if (XPackUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(XPackUser.NAME);
        } else if (XPackSecurityUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(XPackSecurityUser.NAME);
        } else if (AsyncSearchUser.is(user)) {
            output.writeBoolean(true);
            output.writeString(AsyncSearchUser.NAME);
        } else {
            User.writeTo(user, output);
        }
    }
}
