/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to retrieve one or more application privileges.
 */
public final class GetPrivilegesRequest extends ActionRequest implements ApplicationPrivilegesRequest {

    @Nullable
    private String application;
    private String[] privileges;

    private EnumSet<PrivilegeType> includedTypes;

    public GetPrivilegesRequest() {
        privileges = Strings.EMPTY_ARRAY;
        includedTypes = EnumSet.allOf(PrivilegeType.class);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (includedTypes.contains(PrivilegeType.APPLICATION) && privileges == null) {
            validationException = addValidationError(
                "if application privileges are requested, privileges array cannot be null (but may be empty)", validationException);
        }
        return validationException;
    }

    public void privilegeTypes(PrivilegeType first, PrivilegeType... rest) {
        this.includedTypes = EnumSet.of(first, rest);
    }

    public Set<PrivilegeType> privilegeTypes() {
       return Collections.unmodifiableSet(this.includedTypes);
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        application = in.readOptionalString();
        privileges = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.includedTypes = in.readEnumSet(PrivilegeType.class);
        } else {
            this.includedTypes = EnumSet.of(PrivilegeType.APPLICATION);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(application);
        out.writeStringArray(privileges);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeEnumSet(includedTypes);
        }
    }

    public enum PrivilegeType {
        CLUSTER, INDEX, APPLICATION,
    }
}
