/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.shield.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Response containing a User retrieved from the shield administrative index
 */
public class GetUsersResponse extends ActionResponse {
    private List<User> users;

    public GetUsersResponse() {
        this.users = Collections.emptyList();
    }

    public GetUsersResponse(User user) {
        this.users = Collections.singletonList(user);
    }

    public GetUsersResponse(List<User> users) {
        this.users = users;
    }

    public List<User> users() {
        return users;
    }

    public boolean isExists() {
        return users != null && users.size() > 0;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        users = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            users.add(User.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(users == null ? 0 : users.size());
        for (User u : users) {
            User.writeTo(u, out);
        }
    }

}
