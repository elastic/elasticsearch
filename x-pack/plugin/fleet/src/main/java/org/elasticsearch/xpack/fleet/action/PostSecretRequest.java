/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PostSecretRequest extends ActionRequest {

    private final String source;
    private final XContentType xContentType;

    public PostSecretRequest(String source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public PostSecretRequest(StreamInput in) throws IOException {
        super(in);
        this.source = in.readString();
        this.xContentType = in.readEnum(XContentType.class);
    }

    public String source() {
        return source;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(source);
        XContentHelper.writeTo(out, xContentType);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostSecretRequest that = (PostSecretRequest) o;
        return Objects.equals(source, that.source) && xContentType == that.xContentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, xContentType);
    }
}
