/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ProfileHasPrivilegesRequest extends LegacyActionRequest {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ProfileHasPrivilegesRequest, Void> PARSER = new ConstructingObjectParser<>(
        "profile_has_privileges_request",
        false,
        argv -> new ProfileHasPrivilegesRequest((List<String>) argv[0], (PrivilegesToCheck) argv[1])
    );

    static {
        PARSER.declareStringArray(constructorArg(), Fields.UIDS);
        PARSER.declareField(
            constructorArg(),
            parser -> RoleDescriptor.parsePrivilegesToCheck("profile_has_privileges_request", false, parser),
            Fields.PRIVILEGES,
            ObjectParser.ValueType.OBJECT
        );
    }

    private final List<String> uids;
    private final PrivilegesToCheck privilegesToCheck;

    public ProfileHasPrivilegesRequest(List<String> uids, PrivilegesToCheck privilegesToCheck) {
        this.uids = Objects.requireNonNull(uids);
        this.privilegesToCheck = Objects.requireNonNull(privilegesToCheck);
    }

    public ProfileHasPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.uids = in.readStringCollectionAsList();
        this.privilegesToCheck = PrivilegesToCheck.readFrom(in);
    }

    public List<String> profileUids() {
        return uids;
    }

    public PrivilegesToCheck privilegesToCheck() {
        return privilegesToCheck;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return toString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (uids.isEmpty()) {
            validationException = addValidationError("profile uids list must not be empty", validationException);
        }
        return privilegesToCheck.validate(validationException);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(uids);
        privilegesToCheck.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesRequest request = (ProfileHasPrivilegesRequest) o;
        return uids.equals(request.uids) && privilegesToCheck.equals(request.privilegesToCheck);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uids, privilegesToCheck);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "uids=" + uids + "," + "privileges=" + privilegesToCheck + "}";
    }

    public interface Fields {
        ParseField UIDS = new ParseField("uids");
        ParseField PRIVILEGES = new ParseField("privileges");
    }
}
