/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class CreateServiceAccountTokenResponse extends ActionResponse implements ToXContentObject {

    private final String name;
    private final SecureString value;

    private CreateServiceAccountTokenResponse(String name, SecureString value) {
        this.name = name;
        this.value = value;
    }

    public CreateServiceAccountTokenResponse(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.value = in.readOptionalSecureString();
    }

    public String getName() {
        return name;
    }

    public SecureString getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("created", true)
            .field("token")
            .startObject()
            .field("name", name)
            .field("value", value.toString())
            .endObject()
            .endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalSecureString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateServiceAccountTokenResponse that = (CreateServiceAccountTokenResponse) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    public static CreateServiceAccountTokenResponse created(String name, SecureString value) {
        return new CreateServiceAccountTokenResponse(name, value);
    }
}
