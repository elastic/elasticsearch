/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ProfileHasPrivilegesRequest extends ActionRequest {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ProfileHasPrivilegesRequest, Void> PARSER = new ConstructingObjectParser<>(
        ProfileHasPrivilegesAction.NAME,
        false,
        arg -> new ProfileHasPrivilegesRequest((List<String>) arg[0], (RoleDescriptor) arg[1])
    );
    static {
        PARSER.declareStringArray(constructorArg(), Fields.UIDS);
        PARSER.declareField(
            constructorArg(),
            parser -> RoleDescriptor.parsePrivilegesCheck(ProfileHasPrivilegesAction.NAME, parser),
            Fields.PRIVILEGES,
            ObjectParser.ValueType.OBJECT
        );
    }

    private List<String> uids;
    private String[] clusterPrivileges;
    private IndicesPrivileges[] indexPrivileges;
    private RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges;

    public ProfileHasPrivilegesRequest(List<String> uids, RoleDescriptor privilegesToCheck) {
        this.uids = uids;
        this.clusterPrivileges = privilegesToCheck.getClusterPrivileges();
        this.indexPrivileges = privilegesToCheck.getIndicesPrivileges();
        this.applicationPrivileges = privilegesToCheck.getApplicationPrivileges();
    }

    public ProfileHasPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.uids = in.readStringList();
        this.clusterPrivileges = in.readStringArray();
        this.indexPrivileges = in.readArray(IndicesPrivileges::new, IndicesPrivileges[]::new);
        this.applicationPrivileges = in.readArray(
            RoleDescriptor.ApplicationResourcePrivileges::new,
            RoleDescriptor.ApplicationResourcePrivileges[]::new
        );
    }

    public List<String> profileUids() {
        return uids;
    }

    public String[] clusterPrivileges() {
        return clusterPrivileges;
    }

    public IndicesPrivileges[] indexPrivileges() {
        return indexPrivileges;
    }

    public RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges() {
        return applicationPrivileges;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (uids == null) {
            validationException = addValidationError("profile uids must not be null", validationException);
        } else if (uids.isEmpty()) {
            validationException = addValidationError("profile uids list must not be empty", validationException);
        }
        return HasPrivilegesRequest.validateActionRequestPrivileges(
            validationException,
            clusterPrivileges,
            indexPrivileges,
            applicationPrivileges
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(uids);
        out.writeStringArray(clusterPrivileges);
        out.writeArray(IndicesPrivileges::write, indexPrivileges);
        out.writeArray(RoleDescriptor.ApplicationResourcePrivileges::write, applicationPrivileges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesRequest that = (ProfileHasPrivilegesRequest) o;
        return Objects.equals(uids, that.uids)
            && Arrays.equals(clusterPrivileges, that.clusterPrivileges)
            && Arrays.equals(indexPrivileges, that.indexPrivileges)
            && Arrays.equals(applicationPrivileges, that.applicationPrivileges);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(uids);
        result = 31 * result + Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(indexPrivileges);
        result = 31 * result + Arrays.hashCode(applicationPrivileges);
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "uids="
            + uids
            + ","
            + "privileges="
            + "{"
            + "cluster="
            + Arrays.toString(clusterPrivileges)
            + ","
            + "index="
            + Arrays.toString(indexPrivileges)
            + ","
            + "application="
            + Arrays.toString(applicationPrivileges)
            + "}"
            + "}";
    }

    public interface Fields {
        ParseField UIDS = new ParseField("uids");
        ParseField PRIVILEGES = new ParseField("privileges");
    }
}
