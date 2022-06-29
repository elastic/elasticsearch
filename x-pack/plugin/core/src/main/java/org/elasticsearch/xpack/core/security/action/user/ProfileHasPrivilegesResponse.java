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
import java.util.Objects;
import java.util.Set;

public class ProfileHasPrivilegesResponse extends ActionResponse implements ToXContentObject {

    private Set<String> hasPrivilegeUids;
    private Set<String> errorUids;

    public ProfileHasPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.hasPrivilegeUids = in.readSet(StreamInput::readString);
        this.errorUids = in.readSet(StreamInput::readString);
    }

    public ProfileHasPrivilegesResponse(Set<String> hasPrivilegeUids, Set<String> errorUids) {
        super();
        this.hasPrivilegeUids = Objects.requireNonNull(hasPrivilegeUids);
        this.errorUids = Objects.requireNonNull(errorUids);
    }

    public Set<String> hasPrivilegeUids() {
        return hasPrivilegeUids;
    }

    public Set<String> errorUids() {
        return errorUids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesResponse that = (ProfileHasPrivilegesResponse) o;
        return hasPrivilegeUids.equals(that.hasPrivilegeUids) && errorUids.equals(that.errorUids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasPrivilegeUids, errorUids);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().stringListField("has_privilege_uids", hasPrivilegeUids);
        if (false == errorUids.isEmpty()) {
            xContentBuilder.stringListField("error_uids", errorUids);
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(hasPrivilegeUids);
        out.writeStringCollection(errorUids);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "has_privilege_uids=" + hasPrivilegeUids + ", error_uids=" + errorUids + "}";
    }
}
