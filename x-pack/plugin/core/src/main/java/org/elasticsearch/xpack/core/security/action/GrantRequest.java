/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class GrantRequest extends UntypedActionRequest {
    protected final Grant grant;
    private final Set<String> supportedGrantTypes;

    protected GrantRequest(Set<String> supportedGrantTypes) {
        this.grant = new Grant();
        this.supportedGrantTypes = Set.copyOf(supportedGrantTypes);
    }

    public Grant getGrant() {
        return grant;
    }

    @Override
    public ActionRequestValidationException validate() {
        return validate(null);
    }

    protected final ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (grant.getType() != null && supportedGrantTypes.contains(grant.getType()) == false) {
            return addValidationError("grant_type [" + grant.getType() + "] is not supported", validationException);
        }
        return grant.validate(validationException);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
