/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class CreateServiceAccountTokenResponse extends ActionResponse implements ToXContentObject {

    private final boolean created;
    @Nullable
    private final String name;
    @Nullable
    private final SecureString value;

    private CreateServiceAccountTokenResponse(boolean created, String name, SecureString value) {
        this.created = created;
        this.name = name;
        this.value = value;
    }

    public CreateServiceAccountTokenResponse(StreamInput in) throws IOException {
        super(in);
        this.created = in.readBoolean();
        this.name = in.readOptionalString();
        this.value = in.readOptionalSecureString();
    }

    public boolean isCreated() {
        return created;
    }

    public String getName() {
        return name;
    }

    public SecureString getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("created", created);
        if (created) {
            builder.field("token");
            builder.startObject();
            builder.field("name", name);
            builder.field("value", value.toString());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(created);
        out.writeOptionalString(name);
        out.writeOptionalSecureString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CreateServiceAccountTokenResponse that = (CreateServiceAccountTokenResponse) o;
        return created == that.created && Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, name, value);
    }

    public static CreateServiceAccountTokenResponse created(String name, SecureString value) {
        return new CreateServiceAccountTokenResponse(true, name, value);
    }
}
