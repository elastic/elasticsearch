/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Base request for all SQL-related requests.
 * <p>
 * Contains information about the client mode that can be used to generate different responses based on the caller type.
 */
public abstract class AbstractSqlRequest extends ActionRequest implements ToXContent {

    private RequestInfo requestInfo;

    protected AbstractSqlRequest() {
        this.requestInfo = new RequestInfo(Mode.PLAIN);
    }

    protected AbstractSqlRequest(RequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    protected AbstractSqlRequest(StreamInput in) throws IOException {
        super(in);
        Mode mode = in.readEnum(Mode.class);
        String clientId = in.readOptionalString();
        String clientVersion = in.readOptionalString();
        requestInfo = new RequestInfo(mode, clientId, clientVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requestInfo == null || requestInfo.mode() == null) {
            validationException = addValidationError("[mode] is required", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestInfo.mode());
        out.writeOptionalString(requestInfo.clientId());
        out.writeOptionalString(requestInfo.version() == null ? null : requestInfo.version().toString());
    }

    public RequestInfo requestInfo() {
        return requestInfo;
    }
    
    public void requestInfo(RequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    public Mode mode() {
        return requestInfo.mode();
    }

    public void mode(Mode mode) {
        this.requestInfo.mode(mode);
    }

    public void mode(String mode) {
        this.requestInfo.mode(Mode.fromString(mode));
    }
    
    public String clientId() {
        return requestInfo.clientId();
    }

    public void clientId(String clientId) {
        this.requestInfo.clientId(clientId);
    }

    public void version(String clientVersion) {
        requestInfo.version(clientVersion);
    }

    public SqlVersion version() {
        return requestInfo.version();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSqlRequest that = (AbstractSqlRequest) o;
        return Objects.equals(requestInfo, that.requestInfo);
    }

    @Override
    public int hashCode() {
        return requestInfo.hashCode();
    }

}
