/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collection;

/**
 * Response containing a User retrieved from the security index
 */
public class GetUsersResponse extends ActionResponse {

    private User[] users;

    public GetUsersResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        if (size < 0) {
            users = null;
        } else {
            users = new User[size];
            for (int i = 0; i < size; i++) {
                users[i] = User.readFrom(in);
            }
        }
    }

    public GetUsersResponse(User... users) {
        this.users = users;
    }

    public GetUsersResponse(Collection<User> users) {
        this(users.toArray(new User[users.size()]));
    }

    public User[] users() {
        return users;
    }

    public boolean hasUsers() {
        return users != null && users.length > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(users == null ? -1 : users.length);
        if (users != null) {
            for (User user : users) {
                User.writeTo(user, out);
            }
        }
    }

}
