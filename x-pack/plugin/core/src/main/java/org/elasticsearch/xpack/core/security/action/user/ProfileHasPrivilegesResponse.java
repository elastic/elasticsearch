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

    private String[] uids;

    public ProfileHasPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.uids = in.readStringArray();
    }

    public ProfileHasPrivilegesResponse() {
        this(new String[0]);
    }

    public ProfileHasPrivilegesResponse(String[] uids) {
        super();
        this.uids = uids;
    }

    public String[] uids() {
        return uids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesResponse that = (ProfileHasPrivilegesResponse) o;
        return Arrays.equals(uids, that.uids);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(uids);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().array("_has_privileges", uids).endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(uids);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "uids=" + Arrays.toString(uids) + "}";
    }
}
