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
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProfileHasPrivilegesResponse extends ActionResponse implements ToXContentObject {

    private Set<String> hasPrivilegeUids;
    private final Map<String, Exception> errors;

    public ProfileHasPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.hasPrivilegeUids = in.readSet(StreamInput::readString);
        this.errors = in.readMap(StreamInput::readString, StreamInput::readException);
    }

    public ProfileHasPrivilegesResponse(Set<String> hasPrivilegeUids, Map<String, Exception> errors) {
        super();
        this.hasPrivilegeUids = Objects.requireNonNull(hasPrivilegeUids);
        this.errors = Objects.requireNonNull(errors);
    }

    public Set<String> hasPrivilegeUids() {
        return hasPrivilegeUids;
    }

    public Map<String, Exception> errors() {
        return errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesResponse that = (ProfileHasPrivilegesResponse) o;
        // Only compare the keys (profile uids) of the errors, actual error types do not matter
        return hasPrivilegeUids.equals(that.hasPrivilegeUids) && errors.keySet().equals(that.errors.keySet());
    }

    @Override
    public int hashCode() {
        // Only include the keys (profile uids) of the errors, actual error types do not matter
        return Objects.hash(hasPrivilegeUids, errors.keySet());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject().stringListField("has_privilege_uids", hasPrivilegeUids);
        XContentUtils.maybeAddErrorDetails(builder, errors);
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(hasPrivilegeUids);
        out.writeMap(errors, StreamOutput::writeString, StreamOutput::writeException);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "has_privilege_uids=" + hasPrivilegeUids + ", errors=" + errors + "}";
    }
}
