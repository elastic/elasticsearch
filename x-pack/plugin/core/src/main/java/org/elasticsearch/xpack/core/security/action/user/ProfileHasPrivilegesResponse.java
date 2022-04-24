/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

public class ProfileHasPrivilegesResponse extends ActionResponse implements ToXContentObject {

    private String[] hasPrivilegeUids;
    private String[] errorUids;

    public ProfileHasPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.hasPrivilegeUids = in.readStringArray();
        this.errorUids = in.readStringArray();
    }

    public ProfileHasPrivilegesResponse(String[] hasPrivilegeUids, String[] errorUids) {
        super();
        this.hasPrivilegeUids = hasPrivilegeUids;
        this.errorUids = errorUids;
    }

    public ProfileHasPrivilegesResponse() {
        this(new String[0], new String[0]);
    }

    public String[] hasPrivilegeUids() {
        return hasPrivilegeUids;
    }

    public String[] errorUids() {
        return errorUids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesResponse that = (ProfileHasPrivilegesResponse) o;
        return Arrays.equals(hasPrivilegeUids, that.hasPrivilegeUids) && Arrays.equals(errorUids, that.errorUids);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(hasPrivilegeUids);
        result = 31 * result + Arrays.hashCode(errorUids);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().array("has_privilege_uids", hasPrivilegeUids).array("error_uids", errorUids).endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(hasPrivilegeUids);
        out.writeStringArray(errorUids);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "has_privilege_uids="
            + Arrays.toString(hasPrivilegeUids)
            + ", failure_uids="
            + Arrays.toString(errorUids)
            + "}";
    }
}
