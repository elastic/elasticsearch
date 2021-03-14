/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public final class EncryptedRepositoryChangePasswordResponse extends ActionResponse implements StatusToXContentObject, ToXContentObject {

    private String retiredPasswordName;

    public EncryptedRepositoryChangePasswordResponse(StreamInput in) throws IOException {
        super(in);
        this.retiredPasswordName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(retiredPasswordName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("retired_password_name", retiredPasswordName)
                .endObject();
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }
}
