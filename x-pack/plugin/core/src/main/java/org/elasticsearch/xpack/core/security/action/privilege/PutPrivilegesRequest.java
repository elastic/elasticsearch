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
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object to put a one or more application privileges.
 */
public class PutPrivilegesRequest extends ActionRequest implements WriteRequest<PutPrivilegesRequest> {

    private List<ApplicationPrivilege> privileges;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public PutPrivilegesRequest() {
        privileges = Collections.emptyList();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        for (ApplicationPrivilege privilege : privileges) {
            if (privilege.name().size() != 1) {
                validationException = addValidationError("application privileges must have a single name (found " +
                        privilege.name() + ")", validationException);
            }
            if (MetadataUtils.containsReservedMetadata(privilege.getMetadata())) {
                validationException = addValidationError("metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX
                        + "] (in privilege " + privilege.name() + ")", validationException);
            }
        }
        return validationException;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public PutPrivilegesRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    public List<ApplicationPrivilege> getPrivileges() {
        return Collections.unmodifiableList(privileges);
    }

    public void setPrivileges(Collection<ApplicationPrivilege> privileges) {
        this.privileges = new ArrayList<>(privileges);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{[" + privileges.stream().map(Strings::toString).collect(Collectors.joining(","))
                + "];" + refreshPolicy + "}";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        privileges = in.readList(ApplicationPrivilege::readFrom);
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(privileges);
        refreshPolicy.writeTo(out);
    }
}
