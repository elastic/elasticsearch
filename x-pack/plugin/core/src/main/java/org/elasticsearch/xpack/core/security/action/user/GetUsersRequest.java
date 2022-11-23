/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to retrieve a native user.
 */
public class GetUsersRequest extends ActionRequest implements UserRequest {

    private String[] usernames;
    private boolean withProfileUid;

    public GetUsersRequest(StreamInput in) throws IOException {
        super(in);
        usernames = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_8_5_0)) {
            withProfileUid = in.readBoolean();
        } else {
            withProfileUid = false;
        }
    }

    public GetUsersRequest() {
        usernames = Strings.EMPTY_ARRAY;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (usernames == null) {
            validationException = addValidationError("usernames cannot be null", validationException);
        }
        return validationException;
    }

    public void usernames(String... usernames) {
        this.usernames = usernames;
    }

    @Override
    public String[] usernames() {
        return usernames;
    }

    public String[] getUsernames() {
        return usernames;
    }

    public void setUsernames(String[] usernames) {
        this.usernames = usernames;
    }

    public boolean isWithProfileUid() {
        return withProfileUid;
    }

    public void setWithProfileUid(boolean withProfileUid) {
        this.withProfileUid = withProfileUid;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(usernames);
        if (out.getVersion().onOrAfter(Version.V_8_5_0)) {
            out.writeBoolean(withProfileUid);
        }
    }

}
