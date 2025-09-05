/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Map;

public final class UpdateApiKeyRequest extends BaseSingleUpdateApiKeyRequest {
    public static UpdateApiKeyRequest usingApiKeyId(final String id) {
        return new UpdateApiKeyRequest(id, null, null, null);
    }

    public UpdateApiKeyRequest(
        final String id,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata,
        @Nullable final TimeValue expiration
    ) {
        super(roleDescriptors, metadata, expiration, id);
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.REST;
    }

}
