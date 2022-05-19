/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class GrantRequest extends ActionRequest {
    protected final Grant grant;

    public GrantRequest() {
        this.grant = new Grant();
    }

    public GrantRequest(StreamInput in) throws IOException {
        super(in);
        this.grant = new Grant(in);
    }

    public Grant getGrant() {
        return grant;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        grant.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return grant.validate(null);
    }
}
