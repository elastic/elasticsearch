/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class BaseBulkUpdateApiKeyRequest extends BaseUpdateApiKeyRequest {

    private final List<String> ids;

    public BaseBulkUpdateApiKeyRequest(
        final List<String> ids,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata,
        @Nullable final TimeValue expiration
    ) {
        super(roleDescriptors, metadata, expiration);
        this.ids = Objects.requireNonNull(ids, "API key IDs must not be null");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (ids.isEmpty()) {
            validationException = addValidationError("Field [ids] cannot be empty", validationException);
        }
        return validationException;
    }

    public List<String> getIds() {
        return ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass() || super.equals(o)) return false;

        BaseBulkUpdateApiKeyRequest that = (BaseBulkUpdateApiKeyRequest) o;
        return Objects.equals(getIds(), that.getIds())
            && Objects.equals(metadata, that.metadata)
            && Objects.equals(expiration, that.expiration)
            && Objects.equals(roleDescriptors, that.roleDescriptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIds(), expiration, metadata, roleDescriptors);
    }
}
