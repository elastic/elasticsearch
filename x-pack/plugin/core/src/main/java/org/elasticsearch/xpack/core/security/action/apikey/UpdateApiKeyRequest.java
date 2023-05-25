/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class UpdateApiKeyRequest extends BaseSingleUpdateApiKeyRequest {
    public static UpdateApiKeyRequest usingApiKeyId(final String id) {
        return new UpdateApiKeyRequest(id, null, null);
    }

    public UpdateApiKeyRequest(
        final String id,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata
    ) {
        super(roleDescriptors, metadata, id);
    }

    public UpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.REST;
    }
}
