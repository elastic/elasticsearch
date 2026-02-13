/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to retrieve one or more application privileges.
 */
public final class GetPrivilegesRequest extends LegacyActionRequest implements ApplicationPrivilegesRequest {

    @Nullable
    private String application;
    private String[] privileges;

    public GetPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        application = in.readOptionalString();
        privileges = in.readStringArray();
    }

    public GetPrivilegesRequest() {
        privileges = Strings.EMPTY_ARRAY;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (privileges == null) {
            validationException = addValidationError("privileges cannot be null", validationException);
        }
        return validationException;
    }

    public void application(String application) {
        this.application = application;
    }

    public String application() {
        return this.application;
    }

    @Override
    public Collection<String> getApplicationNames() {
        return application == null ? Collections.emptySet() : Collections.singleton(application);
    }

    public void privileges(String... privileges) {
        this.privileges = privileges;
    }

    public String[] privileges() {
        return this.privileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(application);
        out.writeStringArray(privileges);
    }
}
