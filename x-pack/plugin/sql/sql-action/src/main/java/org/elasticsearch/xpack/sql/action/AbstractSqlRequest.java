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

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Base request for all SQL-related requests.
 * <p>
 * Contains information about the client mode that can be used to generate different responses based on the caller type.
 */
public abstract class AbstractSqlRequest extends ActionRequest implements ToXContent {

    private RequestInfo reqParams;

    protected AbstractSqlRequest() {

    }

    protected AbstractSqlRequest(RequestInfo params) {
        this.reqParams = params;
    }

    protected AbstractSqlRequest(StreamInput in) throws IOException {
        super(in);
        Mode mode = in.readEnum(Mode.class);
        String clientId = in.readOptionalString();
        reqParams = new RequestInfo(mode, clientId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (reqParams == null || reqParams.mode() == null) {
            validationException = addValidationError("[mode] is required", validationException);
        }
        return validationException;
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(reqParams.mode());
        out.writeOptionalString(reqParams.clientId());
    }
    
    public RequestInfo reqParams() {
        return reqParams;
    }
    
    public void reqParams(RequestInfo reqParams) {
        this.reqParams = reqParams;
    }

    public Mode mode() {
        return reqParams.mode();
    }

    public void mode(Mode mode) {
        this.reqParams.mode(mode);
    }

    public void mode(String mode) {
        this.reqParams.mode(Mode.fromString(mode));
    }
    
    public String clientId() {
        return reqParams.clientId();
    }

    public void clientId(String clientId) {
        this.reqParams.clientId(clientId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSqlRequest that = (AbstractSqlRequest) o;
        return Objects.equals(reqParams, that.reqParams);
    }

    @Override
    public int hashCode() {
        return reqParams.hashCode();
    }

}
