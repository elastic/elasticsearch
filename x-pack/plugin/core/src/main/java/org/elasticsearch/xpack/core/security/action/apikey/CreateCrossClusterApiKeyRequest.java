/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class CreateCrossClusterApiKeyRequest extends AbstractCreateApiKeyRequest {

    public CreateCrossClusterApiKeyRequest(
        String name,
        CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder,
        @Nullable TimeValue expiration,
        @Nullable Map<String, Object> metadata
    ) {
        super();
        this.name = Objects.requireNonNull(name);
        this.roleDescriptors = List.of(roleDescriptorBuilder.build());
        this.expiration = expiration;
        this.metadata = metadata;
    }

    public CreateCrossClusterApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.expiration = in.readOptionalTimeValue();
        this.roleDescriptors = in.readImmutableList(RoleDescriptor::new);
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        this.metadata = in.readMap();
    }

    @Override
    protected String doReadId(StreamInput in) throws IOException {
        return in.readString();
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.CROSS_CLUSTER;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Assertions.ENABLED) {
            assert roleDescriptors.size() == 1;
            final RoleDescriptor roleDescriptor = roleDescriptors.iterator().next();
            CrossClusterApiKeyRoleDescriptorBuilder.validate(roleDescriptor);
            assert RoleDescriptorRequestValidator.validate(roleDescriptor) == null;
        }
        return super.validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeString(name);
        out.writeOptionalTimeValue(expiration);
        out.writeList(roleDescriptors);
        refreshPolicy.writeTo(out);
        out.writeGenericMap(metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateCrossClusterApiKeyRequest that = (CreateCrossClusterApiKeyRequest) o;
        return Objects.equals(id, that.id)
            && Objects.equals(name, that.name)
            && Objects.equals(expiration, that.expiration)
            && Objects.equals(metadata, that.metadata)
            && Objects.equals(roleDescriptors, that.roleDescriptors)
            && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, expiration, metadata, roleDescriptors, refreshPolicy);
    }

    public static CreateCrossClusterApiKeyRequest withNameAndAccess(String name, String access) throws IOException {
        return new CreateCrossClusterApiKeyRequest(name, CrossClusterApiKeyRoleDescriptorBuilder.parse(access), null, null);
    }
}
