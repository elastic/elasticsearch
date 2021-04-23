/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.CLIENT_ID;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.ID;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.MODE;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.VERSION;

/**
 * Request to manage (status fetching or delete) the SQL asyc resources associated with the id
 */
public class SqlManageAsyncRequest extends AbstractSqlRequest {

    private static final String NAME = "indices:data/read/sql/async_manage";

    private static final ConstructingObjectParser<SqlManageAsyncRequest, Void> PARSER =
        // here the position in "objects" is the same as the fields parser declarations below
        new ConstructingObjectParser<>(NAME, objects -> {
            RequestInfo requestInfo = new RequestInfo(Mode.fromString((String) objects[1]), (String) objects[2]);
            return new SqlManageAsyncRequest(requestInfo, (String) objects[0]);
        });

    static {
        PARSER.declareString(constructorArg(), ID); // "id" is a required constructor parameter
        PARSER.declareString(optionalConstructorArg(), MODE);
        PARSER.declareString(optionalConstructorArg(), CLIENT_ID);
        PARSER.declareString(optionalConstructorArg(), VERSION);
    }

    private String id;

    public SqlManageAsyncRequest() {
    }

    public SqlManageAsyncRequest(RequestInfo requestInfo, String id) {
        super(requestInfo);
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (id() == null) {
            validationException = addValidationError("[" + Protocol.ID_NAME + "] is required", validationException);
        }
        return validationException;
    }

    public String id() {
        return id;
    }

    public SqlManageAsyncRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getDescription() {
        return "SQL manage async " + Protocol.ID_NAME + " [" + id() + "]";
    }

    public SqlManageAsyncRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        SqlManageAsyncRequest that = (SqlManageAsyncRequest) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Protocol.ID_NAME, id);
        if (mode() != null) {
            builder.field(Protocol.MODE_NAME, mode().toString());
        }
        if (clientId() != null) {
            builder.field(Protocol.CLIENT_ID_NAME, clientId());
        }
        if (version() != null) {
            builder.field(Protocol.VERSION_NAME, version().toString());
        }
        return builder;
    }

    public static SqlManageAsyncRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
