/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class CreateCrossClusterApiKeyRequest extends AbstractCreateApiKeyRequest {

    public CreateCrossClusterApiKeyRequest(
        String name,
        CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder,
        @Nullable TimeValue expiration,
        @Nullable Map<String, Object> metadata
    ) {
        super();
        this.name = name;
        this.roleDescriptors = List.of(roleDescriptorBuilder.build());
        this.expiration = expiration;
        this.metadata = metadata;
    }

    public CreateCrossClusterApiKeyRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.CROSS_CLUSTER;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Assertions.ENABLED) {
            assert getRoleDescriptors().size() == 1;
            CrossClusterApiKeyRoleDescriptorBuilder.validate(getRoleDescriptors().iterator().next());
        }
        return super.validate();
    }
}
