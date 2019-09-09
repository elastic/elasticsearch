/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete an application privilege.
 */
public final class DeletePrivilegesRequest extends ActionRequest
    implements ApplicationPrivilegesRequest, WriteRequest<DeletePrivilegesRequest> {

    private String application;
    private String[] privileges;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public DeletePrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        application = in.readString();
        privileges = in.readStringArray();
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public DeletePrivilegesRequest() {
        this(null, Strings.EMPTY_ARRAY);
    }

    public DeletePrivilegesRequest(String application, String[] privileges) {
        this.application = application;
        this.privileges = privileges;
    }

    @Override
    public DeletePrivilegesRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(application)) {
            validationException = addValidationError("application name is missing", validationException);
        }
        if (privileges == null || privileges.length == 0 || Arrays.stream(privileges).allMatch(Strings::isNullOrEmpty)) {
            validationException = addValidationError("privileges are missing", validationException);
        }
        return validationException;
    }

    public void application(String application) {
        this.application = application;
    }

    public String application() {
        return application;
    }

    @Override
    public Collection<String> getApplicationNames() {
        return Collections.singleton(application);
    }

    public String[] privileges() {
        return this.privileges;
    }

    public void privileges(String[] privileges) {
        this.privileges = privileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(application);
        out.writeStringArray(privileges);
        refreshPolicy.writeTo(out);
    }

}
